CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    secret_key TEXT
);

INSERT INTO users (name, email, secret_key) VALUES
('Alice', 'alice@example.com', 'sk_live_123'),
('Bob', 'bob@company.org', 'sk_live_456'),
('Charlie', 'charlie@gmail.com', 'sk_live_789');

