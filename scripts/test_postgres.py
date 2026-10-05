import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="my_realtime_db",
    user="datauser",
    password="password"
)

print("Connected to PostgreSQL successfully!")

conn.close() 