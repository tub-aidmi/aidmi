{{ config(materialized='table') }}

SELECT
    id                                          AS user_id,
    INITCAP(first_name)                         AS firstname,
    INITCAP(last_name)                          AS lastname,
    LOWER(TRIM(email))                          AS email_address,
    phone                                       AS phone_number,
    CASE LOWER(TRIM(status_text))
        WHEN 'active'   THEN 'active'
        WHEN 'inactive' THEN 'inactive'
        WHEN 'archived' THEN 'archived'
        ELSE 'unknown'
    END                                         AS status_enum,
    created_at
FROM {{ source('source_crm', 'contacts') }}
