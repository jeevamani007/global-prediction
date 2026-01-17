from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from typing import List
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from financial import FinancialDomainDetector
import human_resource
from insurence import InsuranceDomainDetector
import os
from government import GovernmentDomainDetector
from bank import BankingDomainDetector  # your banking domain detector class
from banking_dataset_validator import BankingDatasetValidator  # Banking Dataset Validator
from core_banking_validator import CoreBankingValidator  # Core Banking Validation Engine
from database import engine
from sqlalchemy import text
from health_care import HealthcareDomainDetector
from retail import RetailDomainDetector
from space import SpaceDomainDetector
from human_resource import HRDomainDetector
from file_converter import FileConverter
from data_validation_engine import DataValidationEngine
from complete_banking_validator import CompleteBankingValidator
from multi_file_processor import MultiFileProcessor
from application_structure import BankingApplicationStructureGenerator


app = FastAPI(title="Domain Detection API")

# Verify database connection on startup (read-only check)
@app.on_event("startup")
async def startup_event():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM banking_keywords"))
            count = result.scalar()
        print(f"Database connected successfully. Found {count} banking keywords in table.")
    except Exception as e:
        print(f"Warning: Database connection error: {e}")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Upload directory
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".sql"}

# Templates folder
templates = Jinja2Templates(directory="templates")  # make a 'templates' folder and put index.html inside

def is_allowed_file(filename: str):
    return any(filename.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS)


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="No file uploaded")

        if not is_allowed_file(file.filename):
            raise HTTPException(status_code=400, detail="Invalid file type")

        content = await file.read()
        if len(content) == 0:
            raise HTTPException(status_code=400, detail="Empty file")

        # Save the uploaded file
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_path, "wb") as f:
            f.write(content)

        # Convert file to CSV format if needed (handles SQL, Excel, etc.)
        file_converter = None
        original_file_path = file_path
        file_ext = os.path.splitext(file.filename)[1].lower()
        
        if file_ext in ['.sql', '.xlsx', '.xls']:
            try:
                file_converter = FileConverter()
                # Convert to CSV
                file_path = file_converter.convert_to_csv(file_path)
                print(f"File converted to CSV: {file_path}")
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")

        # 🔥 STEP 0: CORE BANKING BUSINESS RULES ENGINE (RUNS FIRST)
        core_banking_rules_result = None
        try:
            from core_banking_business_rules_engine import CoreBankingBusinessRulesEngine
            import pandas as pd
            core_rules_engine = CoreBankingBusinessRulesEngine()
            df_for_rules = pd.read_csv(file_path)
            core_banking_rules_result = core_rules_engine.analyze_dataset(file_path, df_for_rules)
            print(f"Core Banking Business Rules Engine: Analyzed {core_banking_rules_result.get('total_columns', 0)} columns")
        except Exception as e:
            print(f"Warning: Core Banking Business Rules Engine error: {str(e)}")
            import traceback
            traceback.print_exc()
            core_banking_rules_result = {"error": str(e)}

        # Run domain detection – banking first, then others as needed
        try:
            # Banking domain detection
            banking_detector = BankingDomainDetector()
            banking_result = banking_detector.predict(file_path)
            
            # Check if result contains an error
            if isinstance(banking_result, dict) and "error" in banking_result:
                raise HTTPException(status_code=500, detail=f"Banking analysis error: {banking_result['error']}")
            
            # Banking Dataset Validator - run validation on banking datasets
            banking_validator_result = None
            if banking_result and isinstance(banking_result, dict) and banking_result.get("decision") not in (None, "UNKNOWN"):
                try:
                    # Use our new complete banking validator
                    complete_validator = CompleteBankingValidator()
                    banking_validator_result = complete_validator.validate_dataset(file_path)
                    
                    # Map the results to match the expected format for UI
                    if "error" not in banking_validator_result:
                        # Extract column-level validation results
                        column_validation_results = banking_validator_result.get("column_wise_validation", [])
                        
                        columns_result = []
                        for col in column_validation_results:
                            status = col.get("validation_result", "FAIL").upper()
                            
                            columns_result.append({
                                "name": col.get("column_name", "Unknown"),
                                "meaning": col.get("standard_name", "Unknown"),
                                "status": status,
                                "confidence": col.get("confidence_percentage", 0),  # Already in percentage
                                "rules_passed": 1,  # Placeholder - would need actual count
                                "rules_total": 1,   # Placeholder - would need actual count
                                "failures": col.get("detected_issue", []),
                                "applied_rules": [col.get("business_rule", "General Rule")],
                                "reasons": col.get("detected_issue", [])
                            })
                        
                        # Calculate overall dataset confidence
                        summary = banking_validator_result.get("summary", {})
                        avg_confidence = summary.get("overall_confidence", 0)
                        
                        # Determine final decision based on overall confidence
                        overall_confidence = summary.get("overall_confidence", 0)
                        if overall_confidence >= 95:
                            final_decision = "PASS"
                        elif overall_confidence >= 80:
                            final_decision = "PASS WITH WARNINGS"
                        else:
                            final_decision = "FAIL"
                        
                        banking_validator_result = {
                            "final_decision": final_decision,
                            "dataset_confidence": round(avg_confidence, 1),
                            "explanation": f"Banking validation completed. {summary.get('total_columns_analyzed', 0)} columns analyzed, {summary.get('total_passed', 0)} passed, {summary.get('total_failed', 0)} failed.",
                            "columns": columns_result,
                            "relationships": banking_validator_result.get("cross_column_validations", []),
                            "total_records": summary.get("total_records", 0)
                        }
                except Exception as e:
                    print(f"Warning: Complete banking validator error: {str(e)}")
                    # Fall back to original validator
                    try:
                        validator = BankingDatasetValidator()
                        banking_validator_result = validator.validate(file_path)
                    except Exception as fallback_e:
                        print(f"Warning: Fallback banking validator error: {str(fallback_e)}")
                        banking_validator_result = {"error": str(e)}
            
            # Core Banking Validation Engine - run comprehensive validation
            core_banking_validator_result = None
            if banking_result and isinstance(banking_result, dict) and banking_result.get("decision") not in (None, "UNKNOWN"):
                try:
                    core_validator = CoreBankingValidator()
                    core_banking_validator_result = core_validator.validate(file_path)
                except Exception as e:
                    print(f"Warning: Core banking validator error: {str(e)}")
                    core_banking_validator_result = {"error": str(e)}

            # Financial domain detection (only if banking is NOT clearly detected)
            financial_result = None
            banking_decision = None
            if isinstance(banking_result, dict):
                banking_decision = banking_result.get("decision")

            if banking_decision in (None, "UNKNOWN"):
                financial_detector = FinancialDomainDetector()
                financial_result = financial_detector.predict(file_path)

                if isinstance(financial_result, dict) and "error" in financial_result:
                    raise HTTPException(status_code=500, detail=f"Financial analysis error: {financial_result['error']}")
            else:
                # Explicit marker so UI / logs know financial was skipped intentionally
                financial_result = {
                    "domain": "Financial",
                    "decision": "SKIPPED",
                    "reason": "Skipped because Banking domain was already detected for this file.",
                    "confidence_percentage": 0.0,
                    "confidence_out_of_10": 0.0,
                    "qualitative": "Not evaluated"
                }

            insurance_detector = InsuranceDomainDetector()
            insurance_result = insurance_detector.predict(file_path)
            if isinstance(insurance_result, dict) and "error" in insurance_result:
                raise HTTPException(status_code=500, detail=f"Insurance analysis error: {insurance_result['error']}")
            
            government_detector = GovernmentDomainDetector()
            government_result = government_detector.predict(file_path)
            if isinstance(government_result, dict) and "error" in government_result:
                raise HTTPException(
                    status_code=500,
                    detail=f"Government analysis error: {government_result['error']}"
                )
            healthcare_detector = HealthcareDomainDetector()
            healthcare_result = healthcare_detector.predict(file_path)
            if isinstance(healthcare_result, dict) and "error" in healthcare_result:
                raise HTTPException(
                    status_code=500,
                    detail=f"Healthcare analysis error: {healthcare_result['error']}"
                )

            retail_detector = RetailDomainDetector()
            retail_result = retail_detector.predict(file_path)
            if isinstance(retail_result, dict) and "error" in retail_result:
                raise HTTPException(
                    status_code=500,
                    detail=f"Retail analysis error: {retail_result['error']}"
                )

            space_detector = SpaceDomainDetector()
            space_result = space_detector.predict(file_path)
            if isinstance(space_result, dict) and "error" in space_result:
                raise HTTPException(
                    status_code=500,
                    detail=f"Space analysis error: {space_result['error']}"
                )

            human_resource_detect =HRDomainDetector()
            human_resource_result =human_resource_detect.predict(file_path)

            if isinstance(human_resource_result,dict) and  "error"  in human_resource_result:
                raise HTTPException(
                    status_code=500,
                    detail=f'hr analysis error : {human_resource_result["error"]}'
                )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error during analysis: {str(e)}")

        # Clean up temporary CSV file if file was converted
        if file_converter:
            try:
                file_converter.cleanup_temp_files()
            except Exception as e:
                print(f"Warning: Could not clean up temp files: {e}")

        # 🔥 BANKING BLUEPRINT ANALYSIS (UNIFIED)
        banking_blueprint = None
        try:
            from banking_blueprint_engine import BankingBlueprintEngine
            import pandas as pd
            blueprint_engine = BankingBlueprintEngine()
            # For single file, use analyze_file method
            try:
                df = pd.read_csv(file_path)
                blueprint_result = blueprint_engine.analyze_file(file_path, df)
                banking_blueprint = blueprint_result
            except Exception as e:
                print(f"Warning: Could not analyze file for blueprint: {str(e)}")
                # Fallback: try with file path as dict key
                try:
                    df = pd.read_csv(file_path)
                    blueprint_result = blueprint_engine.analyze_multiple_files({file.filename: df})
                    banking_blueprint = blueprint_result
                except Exception as e2:
                    print(f"Warning: Multi-file fallback also failed: {str(e2)}")
                    raise e
        except Exception as e:
            print(f"Warning: Banking blueprint analysis error: {str(e)}")
            import traceback
            traceback.print_exc()
            # Generate minimal blueprint with business rules only
            try:
                import pandas as pd
                from banking_blueprint_engine import BankingBlueprintEngine
                df = pd.read_csv(file_path)
                blueprint_engine = BankingBlueprintEngine()
                # At minimum, generate business rules
                columns = list(df.columns)
                business_rules = blueprint_engine.apply_business_rules(columns, df)
                banking_blueprint = {
                    "domain": "Banking",
                    "confidence_percentage": 50,
                    "application": "Unknown",
                    "application_confidence": 0,
                    "business_rules": business_rules,
                    "error": f"Partial analysis due to: {str(e)}"
                }
            except Exception as fallback_e:
                print(f"Warning: Fallback blueprint generation also failed: {str(fallback_e)}")
                banking_blueprint = {
                    "domain": "Banking",
                    "confidence_percentage": 0,
                    "application": "Unknown",
                    "error": f"Blueprint analysis failed: {str(fallback_e)}"
                }

        # 🔥 APPLICATION STRUCTURE GENERATOR (NEW FEATURE)
        application_structure = None
        try:
            structure_generator = BankingApplicationStructureGenerator()
            application_structure = structure_generator.generate_structure([file_path])
        except Exception as e:
            print(f"Warning: Application structure generation error: {str(e)}")
            import traceback
            traceback.print_exc()
            application_structure = {"error": str(e)}

        return JSONResponse(
            content={
                "message": "File analyzed successfully",
                "filename": file.filename,
                "file_type": "SQL" if file.filename.lower().endswith('.sql') else "CSV/Excel",
                "banking": banking_result,
                "banking_account_validation": banking_result.get("account_number_validation"),
                "banking_account_check": banking_result.get("account_number_check"),
                "banking_account_status": banking_result.get("account_status"),
                "banking_missing_columns": banking_result.get("missing_columns_check"),
                "banking_balance_analysis": banking_result.get("balance_analysis"),
                "banking_opening_debit_credit_detection": banking_result.get("opening_debit_credit_detection"),
                # KYC, PAN, Branch Code, Fraud Detection REMOVED as per specification
                "banking_customer_id_validation": banking_result.get("customer_id_validation"),
                "banking_transaction_validation": banking_result.get("transaction_validation"),
                "banking_transaction_type_validation": banking_result.get("transaction_type_validation"),
                "banking_debit_credit_validation": banking_result.get("debit_credit_validation"),
                "banking_purpose_detection": banking_result.get("purpose_detection"),
                "banking_purpose_report": banking_result.get("purpose_detection"), # Standardize key
                "banking_probability_explanations": banking_result.get("probability_explanations"),
                "banking_transaction_rules": banking_result.get("banking_transaction_rules"),
                "banking_column_purpose_report": banking_result.get("column_purpose_report"),
                "banking_column_mapping": banking_result.get("banking_column_mapping"),
                
                # 🔥 CORE BANKING BUSINESS RULES ENGINE (PRIMARY - RUNS FIRST)
                "core_banking_business_rules": core_banking_rules_result,
                
                # 🔥 BANKING BLUEPRINT (NEW UNIFIED ANALYSIS)
                "banking_blueprint": banking_blueprint,
                
                # 🔥 APPLICATION STRUCTURE GENERATOR (NEW FEATURE)
                "application_structure": application_structure,
                
                # 🔥 CORE BANKING ENGINE RESULTS (PRIMARY OUTPUT - KYC REMOVED)
                "banking_core_analysis": banking_result.get("core_banking_analysis"),
                "banking_core_detected_columns": banking_result.get("core_detected_columns"),
                "banking_core_column_validations": banking_result.get("core_column_validations"),
                "banking_core_cross_validations": banking_result.get("core_cross_validations"),
                "banking_core_validation_summary": banking_result.get("core_validation_summary"),
                
                # 🔥 BANKING DATASET VALIDATOR RESULTS
                "banking_dataset_validator": banking_validator_result,
                
                # 🔥 CORE BANKING VALIDATION ENGINE RESULTS
                "core_banking_validator": core_banking_validator_result,
                
                # 🔥 BANKING APPLICATION TYPE PREDICTION (NEW)
                "banking_application_type": banking_result.get("banking_application_type"),
                
                "financial": financial_result,
                "insurance": insurance_result,
                "government": government_result,
                "healthcare": healthcare_result,
                "retail": retail_result,
                "space":space_result,
                "human_resource":human_resource_result
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


@app.post("/upload-multiple")
async def upload_multiple_files(files: List[UploadFile] = File(...)):
    """Multi-file upload endpoint"""
    try:
        if not files or len(files) == 0:
            raise HTTPException(status_code=400, detail="No files uploaded")
        
        # Validate all files
        file_paths = []
        file_converters = []
        
        for file in files:
            if not file.filename:
                continue
            
            if not is_allowed_file(file.filename):
                raise HTTPException(status_code=400, detail=f"Invalid file type: {file.filename}")
            
            content = await file.read()
            if len(content) == 0:
                continue
            
            # Save the uploaded file
            file_path = os.path.join(UPLOAD_DIR, file.filename)
            with open(file_path, "wb") as f:
                f.write(content)
            
            # Convert file to CSV format if needed
            file_ext = os.path.splitext(file.filename)[1].lower()
            file_converter = None
            
            if file_ext in ['.sql', '.xlsx', '.xls']:
                try:
                    file_converter = FileConverter()
                    file_path = file_converter.convert_to_csv(file_path)
                    file_converters.append(file_converter)
                    print(f"File converted to CSV: {file_path}")
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"Error processing file {file.filename}: {str(e)}")
            
            file_paths.append(file_path)
        
        if not file_paths:
            raise HTTPException(status_code=400, detail="No valid files to process")
        
        # 🔥 STEP 0: CORE BANKING BUSINESS RULES ENGINE (RUNS FIRST FOR EACH FILE)
        core_banking_rules_results = {}
        try:
            from core_banking_business_rules_engine import CoreBankingBusinessRulesEngine
            import pandas as pd
            core_rules_engine = CoreBankingBusinessRulesEngine()
            
            for file_path in file_paths:
                try:
                    df = pd.read_csv(file_path)
                    file_name = os.path.basename(file_path)
                    core_rules_result = core_rules_engine.analyze_dataset(file_path, df)
                    core_banking_rules_results[file_name] = core_rules_result
                except Exception as e:
                    print(f"Warning: Core Banking Rules analysis failed for {file_path}: {str(e)}")
                    core_banking_rules_results[os.path.basename(file_path)] = {"error": str(e)}
        except Exception as e:
            print(f"Warning: Core Banking Business Rules Engine initialization error: {str(e)}")
        
        # Process multiple files using multi-file processor
        try:
            multi_file_processor = MultiFileProcessor()
            result = multi_file_processor.process_files(file_paths)
            
            # Clean up temporary files
            for converter in file_converters:
                try:
                    converter.cleanup_temp_files()
                except Exception as e:
                    print(f"Warning: Could not clean up temp files: {e}")
            
            # Format response to match single-file format (for UI compatibility)
            if result.get("multi_file_mode"):
                # Extract primary banking result for backward compatibility
                banking_result = result.get("banking", {})
                banking_validator_result = result.get("banking_dataset_validator")
                core_banking_validator_result = result.get("core_banking_validator")
                
                # Build response maintaining same structure as single-file
                response = {
                    "message": "Files analyzed successfully",
                    "multi_file_mode": True,
                    "total_files": result.get("total_files", len(file_paths)),
                    # 🔥 CORE BANKING BUSINESS RULES ENGINE (PRIMARY - RUNS FIRST)
                    "core_banking_business_rules": core_banking_rules_results,
                    "banking": banking_result,
                    "banking_dataset_validator": banking_validator_result,
                    "core_banking_validator": core_banking_validator_result,
                    "banking_blueprint": result.get("banking_blueprint"),
                    "application_structure": result.get("application_structure"),  # New: Application structure
                    "domain_detection": result.get("domain_detection"),
                    "primary_keys": result.get("primary_keys"),
                    "foreign_keys": result.get("foreign_keys"),
                    "relationships": result.get("relationships"),
                    "file_relationships": result.get("file_relationships", []),  # File-to-file relationships with explanations
                    "column_relationship_analysis": result.get("column_relationship_analysis", {}),  # Column relationships and domains
                    "overall_verdict": result.get("overall_verdict"),
                    "overall_confidence": result.get("overall_confidence"),
                    "business_explanation": result.get("business_explanation"),
                    "table_results": result.get("table_results", [])
                }
                
                # Add all domain results from first table (for UI compatibility)
                if result.get("table_results") and len(result["table_results"]) > 0:
                    first_table = result["table_results"][0]
                    if first_table.get("status") == "SUCCESS":
                        # Set default domain results (they'll be empty for multi-file)
                        response["financial"] = None
                        response["insurance"] = None
                        response["government"] = None
                        response["healthcare"] = None
                        response["retail"] = None
                        response["space"] = None
                        response["human_resource"] = None
                
                return JSONResponse(content=response)
            else:
                return JSONResponse(content=result)
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error during multi-file analysis: {str(e)}")
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return JSONResponse(content={"status": "ok", "message": "Server is running"})

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Return the index.html page"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/account", response_class=HTMLResponse)
async def account_page(request: Request):
    """Return the account.html page for account number detection results and application structure"""
    return templates.TemplateResponse("account.html", {"request": request})

@app.get("/account.html", response_class=HTMLResponse)
async def account_page_html(request: Request):
    """Return the account.html page (html suffix)"""
    return templates.TemplateResponse("account.html", {"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
