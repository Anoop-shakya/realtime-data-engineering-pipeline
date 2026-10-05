import requests
import psycopg2

# -----------------------------
# 1. Fetch users from API
# -----------------------------

url = "https://randomuser.me/api/?results=10"

response = requests.get(url)

if response.status_code != 200:
    print("API request failed!")
    exit()

data = response.json()

users = data["results"]

print(f"Fetched {len(users)} users from API")


# -----------------------------
# 2. Connect to PostgreSQL
# -----------------------------

conn = psycopg2.connect(
    host="postgres",
    port=5432,
    database="my_realtime_db",
    user="datauser",
    password="password"
)

cursor = conn.cursor()

print("Connected to PostgreSQL")


# -----------------------------
# 3. Insert users
# -----------------------------

insert_query = """
INSERT INTO users
(first_name, last_name, email, gender, country, city, phone)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

for user in users:

    cursor.execute(
        insert_query,
        (
            user["name"]["first"],
            user["name"]["last"],
            user["email"],
            user["gender"],
            user["location"]["country"],
            user["location"]["city"],
            user["phone"]
        )
    )


# -----------------------------
# 4. Save changes
# -----------------------------

conn.commit()

print(f"Inserted {len(users)} users into PostgreSQL")


# -----------------------------
# 5. Close connection
# -----------------------------

cursor.close()
conn.close()

print("Done!")