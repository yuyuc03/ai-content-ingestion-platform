from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://admin:admin123@localhost:5432/ingestion_db")

with open("sql/create_tables.sql", "r") as f:
    sql = f.read()

with engine.connect() as conn:
    conn.execute(text(sql))
    conn.commit()
    print("All tables created successfully")