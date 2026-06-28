from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://admin:admin123@localhost:5432/ingestion_db")

with engine.connect() as conn:
    conn.execute(text("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, msg TEXT)"))
    conn.execute(text("INSERT INTO test_table (msg) VALUES ('hello docker postgres')"))
    conn.commit()
    result = conn.execute(text("SELECT * FROM test_table"))
    for row in result:
        print(row)
