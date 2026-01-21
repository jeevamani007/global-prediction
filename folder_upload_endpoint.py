"""
Folder Upload Endpoint for main.py
Add this code to main.py after the upload_multiple_files endpoint
"""

@app.post("/upload-folders")
async def upload_folders(files: List[UploadFile] = File(...)):
    """
    Folder-based upload endpoint.
    Each folder contains multiple CSV files representing ONE application.
    Folders are processed independently with complete isolation.
    
    File naming format: "folder_name/file.csv"
    Example: "banking_app/accounts.csv", "banking_app/transactions.csv", "loan_app/loans.csv"
    """
    try:
        if not files or len(files) == 0:
            raise HTTPException(status_code=400, detail="No files uploaded")
        
        # Group files by their directory structure
        # Format expected: "folder_name/file.csv" in filename
        folder_files = {}  # {folder_name: [file_paths]}
        temp_folders = []
        
        for file in files:
            if not file.filename:
                continue
            
            # Extract folder name from file path (format: "folder_name/file.csv")
            if '/' in file.filename:
                parts = file.filename.split('/')
                folder_name = parts[0]
                actual_filename = parts[-1]
            else:
                # Default: all files in one folder
                folder_name = "default_folder"
                actual_filename = file.filename
            
            # Create folder if not exists
            folder_path = os.path.join(UPLOAD_DIR, folder_name)
            os.makedirs(folder_path, exist_ok=True)
            
            if folder_name not in temp_folders:
                temp_folders.append(folder_name)
            
            # Save file
            content = await file.read()
            if len(content) == 0:
                continue
            
            file_path = os.path.join(folder_path, actual_filename)
            with open(file_path, "wb") as f:
                f.write(content)
            
            # Track files by folder
            if folder_name not in folder_files:
                folder_files[folder_name] = []
            folder_files[folder_name].append(file_path)
        
        if not folder_files:
            raise HTTPException(status_code=400, detail="No valid files to process")
        
        # Process each folder independently
        from folder_based_application_analyzer import FolderBasedApplicationAnalyzer
        from application_type_detector import ApplicationTypeDetector
        from dynamic_business_rules_from_data import generate_dynamic_business_rules
        from banking_business_rules_summarizer import summarize_banking_business_rules
        from application_purpose_analyzer import ApplicationPurposeAnalyzer
        from file_relationship_analyzer import FileRelationshipAnalyzer
        
        folder_analyzer = FolderBasedApplicationAnalyzer()
        app_type_detector = ApplicationTypeDetector()
        purpose_analyzer = ApplicationPurposeAnalyzer()
        
        # Results for each folder
        folders_results = {}
        
        for folder_name, file_paths in folder_files.items():
            try:
                folder_path = os.path.join(UPLOAD_DIR, folder_name)
                
                # 1. Analyze folder structure and CSV files
                folder_analysis = folder_analyzer.analyze_folder(folder_path)
                
                if 'error' in folder_analysis:
                    folders_results[folder_name] = {
                        'error': folder_analysis['error'],
                        'folder_name': folder_name
                    }
                    continue
                
                csv_files_data = folder_analysis.get('csv_files_data', {})
                if not csv_files_data:
                    folders_results[folder_name] = {
                        'error': 'No CSV files could be loaded',
                        'folder_name': folder_name
                    }
                    continue
                
                # 2. Detect application type using patterns
                cross_file_relationships = folder_analysis.get('cross_file_relationships', [])
                app_type_result = app_type_detector.detect_type(
                    csv_files_data,
                    relationships=cross_file_relationships
                )
                
                # 3. Apply business rules to each CSV file
                business_rules_per_file = {}
                business_rules_summaries = {}
                
                for filename in folder_analysis['csv_files']:
                    file_path = os.path.join(folder_path, filename)
                    try:
                        # Generate dynamic business rules from observed data
                        dynamic_rules = generate_dynamic_business_rules(file_path)
                        business_rules_per_file[filename] = dynamic_rules
                        
                        # Generate high-level summary
                        if dynamic_rules and dynamic_rules.get('columns'):
                            summary = summarize_banking_business_rules(dynamic_rules)
                            business_rules_summaries[filename] = summary
                    except Exception as e:
                        print(f"Warning: Could not generate business rules for {filename}: {str(e)}")
                        business_rules_per_file[filename] = {'error': str(e)}
                
                # 4. Analyze file relationships with detailed explanations
                relationship_analyzer = FileRelationshipAnalyzer()
                detailed_relationships = relationship_analyzer.analyze_file_relationships(csv_files_data)
                # detailed_relationships is a list of relationship dicts
                
                # 5. Generate application purpose explanation
                application_purpose = None
                try:
                    combined_rules = {
                        'multi_file': True,
                        'files': business_rules_per_file
                    }
                    application_purpose = purpose_analyzer.analyze_from_rules_data(
                        combined_rules,
                        relationships=detailed_relationships  # Pass the list directly
                    )
                except Exception as e:
                    print(f"Warning: Could not generate application purpose for {folder_name}: {str(e)}")
                
                # 6. Detect issues and warnings
                issues = []
                warnings = []
                
                # Check for validation issues in business rules
                for filename, rules in business_rules_per_file.items():
                    if 'error' in rules:
                        issues.append({
                            'file': filename,
                            'type': 'Analysis Error',
                            'message': rules['error']
                        })
                    elif rules.get('columns'):
                        for col in rules['columns']:
                            if col.get('validation_issues'):
                                for issue in col['validation_issues']:
                                    issues.append({
                                        'file': filename,
                                        'column': col.get('column_name'),
                                        'type': 'Validation Issue',
                                        'message': issue
                                    })
                
                # Check for low confidence application type detection
                if app_type_result['confidence'] < 60:
                    warnings.append({
                        'type': 'Low Confidence',
                        'message': f"Application type detected with low confidence ({app_type_result['confidence']}%)."
                    })
                
                # 7. Build comprehensive folder result
                folders_results[folder_name] = {
                    'folder_name': folder_name,
                    'folder_path': folder_path,
                    'application_type': app_type_result,
                    'csv_files': folder_analysis['csv_files'],
                    'file_roles': folder_analysis.get('file_roles', {}),
                    'file_relationships': detailed_relationships,  # already a list
                    'cross_file_relationships': cross_file_relationships,
                    'business_rules': business_rules_per_file,
                    'business_rules_summaries': business_rules_summaries,
                    'application_purpose': application_purpose,
                    'schema_patterns': folder_analysis.get('schema_patterns', {}),
                    'total_files': folder_analysis.get('total_files', 0),
                    'total_rows': folder_analysis.get('total_rows', 0),
                    'total_columns': folder_analysis.get('total_columns', 0),
                    'unique_column_count': folder_analysis.get('unique_column_count', 0),
                    'issues': issues,
                    'warnings': warnings,
                    'status': 'success' if not issues else 'warning'
                }
                
            except Exception as e:
                print(f"Error processing folder {folder_name}: {str(e)}")
                import traceback
                traceback.print_exc()
                folders_results[folder_name] = {
                    'folder_name': folder_name,
                    'error': f"Error processing folder: {str(e)}",
                    'status': 'error'
                }
        
        # Build response
        response = {
            'message': 'Folders analyzed successfully',
            'multi_folder_mode': True,
            'total_folders': len(folders_results),
            'folders': folders_results
        }
        
        return JSONResponse(content=response)
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")
