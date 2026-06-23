CREATE SCHEMA IF NOT EXISTS fixture_mock_src;
SET search_path TO fixture_mock_src;

CREATE TABLE contacts (
    id integer PRIMARY KEY,
    first_name text,
    last_name text,
    email text,
    phone text,
    status_text text,
    created_at text
);

INSERT INTO contacts (id, first_name, last_name, email, phone, status_text, created_at) VALUES
  (1, 'John', 'Doe', 'john@example.com', '+15551234567', 'active', '2024-01-15T10:00:00Z'),
  (2, 'JANE', 'SMITH', 'jane@example.com', '555-1234', 'ACTIVE', '2024-01-16T11:00:00Z'),
  (3, 'Alice', 'Doe', '  alice@example.com  ', '(555) 333-3333', 'Active', '2024-01-17T12:00:00Z'),
  (4, 'Bob', 'X', 'bob@example.com', '5552222222', 'archived', '2024-01-18T13:00:00Z'),
  (5, 'Carol', 'Y', 'carol@example.com', '5551111111', 'INACTIVE', '2024-01-19T14:00:00Z'),
  (6, 'Dan', NULL, NULL, NULL, 'active', '2024-01-20T15:00:00Z'),
  (7, 'Eve', 'Z', 'eve@example.com', '555-9999', 'frobnicated', '2024-01-21T16:00:00Z'),
  (8, 'Frank', 'Q', 'frank@example.com', '5550000000', 'active', '2024-01-22T17:00:00Z');
