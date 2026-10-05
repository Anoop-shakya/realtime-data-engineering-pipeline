CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    gender VARCHAR(20),
    country VARCHAR(100),
    city VARCHAR(100),
    phone VARCHAR(50)
);