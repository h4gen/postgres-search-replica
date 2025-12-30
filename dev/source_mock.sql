CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    price DECIMAL(10, 2)
);

INSERT INTO products (name, description, price) VALUES
('AI Assistant Bot', 'A state-of-the-art autonomous AI agent for productivity.', 49.99),
('Smart Fitness Watch', 'Water-resistant fitness tracker with heart rate monitoring.', 129.50),
('Wireless Noise-Canceling Headphones', 'Premium audio quality with active noise cancellation.', 299.00);
