from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from financial import FinancialDomainDetector
import human_resource
from insurence import InsuranceDomainDetector
import os
from government import GovernmentDomainDetector
from bank import BankingDomainDetector  # your banking domain detector class
from database import engine
from sqlalchemy import text
from health_care import HealthcareDomainDetector
from retail import RetailDomainDetector
from space import SpaceDomainDetector
from human_resource import HRDomainDetector


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

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

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

        # Run domain detection for both banking and financial
        try:
            # Banking domain detection
            banking_detector = BankingDomainDetector()
            banking_result = banking_detector.predict(file_path)
            
            # Check if result contains an error
            if isinstance(banking_result, dict) and "error" in banking_result:
                raise HTTPException(status_code=500, detail=f"Banking analysis error: {banking_result['error']}")
            
            # Financial domain detection
            financial_detector = FinancialDomainDetector()
            financial_result = financial_detector.predict(file_path)

            if isinstance(financial_result, dict) and "error" in financial_result:
                raise HTTPException(status_code=500, detail=f"Financial analysis error: {financial_result['error']}")

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

        return JSONResponse(
            content={
                "message": "File analyzed successfully",
                "filename": file.filename,
                "banking": banking_result,
                "banking_account_validation": banking_result.get("account_number_validation"),
                "banking_account_check": banking_result.get("account_number_check"),
                "banking_account_status": banking_result.get("account_status"),
                "banking_missing_columns": banking_result.get("missing_columns_check"),
                "banking_balance_analysis": banking_result.get("balance_analysis"),
                "banking_kyc_verification": banking_result.get("kyc_verification"),
                "banking_customer_id_validation": banking_result.get("customer_id_validation"),
                "banking_transaction_validation": banking_result.get("transaction_validation"),
                "banking_debit_credit_validation": banking_result.get("debit_credit_validation"),
                "banking_fraud_detection": banking_result.get("fraud_detection"),
                "banking_foreign_key_check": banking_result.get("foreign_key_check"),
                "banking_purpose_detection": banking_result.get("purpose_detection"),
                "banking_final_decision": banking_result.get("final_decision"),
                "banking_risk_assessment": banking_result.get("risk_assessment"),
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
    """Return the account.html page for account number detection results"""
    return templates.TemplateResponse("account.html", {"request": request})

@app.get("/account.html", response_class=HTMLResponse)
async def account_page_html(request: Request):
    """Return the account.html page (html suffix)"""
    return templates.TemplateResponse("account.html", {"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
