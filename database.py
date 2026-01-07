from sqlalchemy import create_engine

# ✅ Database connection
DATABASE_URL = "postgresql+psycopg2://postgres:Jeeva%40123@localhost:5432/business_prediction"

# Create engine for raw SQL queries (no ORM needed)
engine = create_engine(DATABASE_URL)
