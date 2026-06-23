-- Target schema definition (structure only — no data)

CREATE TABLE users (
    user_id integer PRIMARY KEY,
    firstname text,
    lastname text,
    email_address text,
    phone_number text,
    status_enum text CHECK (status_enum IN ('active', 'inactive', 'archived', 'unknown')),
    created_at text
);
