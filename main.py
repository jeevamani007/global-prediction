from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from financial import FinancialDomainDetector
from insurence import InsuranceDomainDetector
import os
from government import GovernmentDomainDetector
from bank import BankingDomainDetector  # your banking domain detector class
from database import engine
from sqlalchemy import text

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
                    detail=f"Government analysis error: {government_result['error']}",
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
                "financial": financial_result,
                "insurance": insurance_result,
                "government": government_result
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
