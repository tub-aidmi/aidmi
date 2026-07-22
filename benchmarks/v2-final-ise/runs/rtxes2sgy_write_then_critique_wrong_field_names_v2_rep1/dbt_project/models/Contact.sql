{{ config(materialized='table') }}

WITH contact_source AS (
    SELECT
        ap_id AS contact_id,
        vorname AS first_name,
        nachname AS last_name,
        email_adresse AS email,
        telefonnummer AS phone,
        position AS title,
        funktion AS role,
        sprache AS language,
        kunde AS customer_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
),
account_mapping AS (
    SELECT
        kunden_nr AS customer_id,
        kunden_nr AS account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)
SELECT
    contact_source.contact_id AS "Id",
    contact_source.first_name AS "FirstName",
    contact_source.last_name AS "LastName",
    contact_source.email AS "Email",
    contact_source.phone AS "Phone",
    contact_source.title AS "Title",
    CASE
        WHEN LOWER(TRIM(contact_source.role)) IN ('entscheidungsträger', 'decision maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(contact_source.role)) IN ('endbenutzer', 'end user') THEN 'End User'
        WHEN LOWER(TRIM(contact_source.role)) IN ('technischer kontakt', 'technical contact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(contact_source.role)) IN ('executive sponsor', 'geschäftsführung') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(contact_source.language)) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(contact_source.language)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(contact_source.language)) IN ('FR', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(contact_source.language)) IN ('ES', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(contact_source.language)) IN ('IT', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    account_mapping.account_id AS "AccountId",
    contact_source.contact_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_source
LEFT JOIN account_mapping ON contact_source.customer_id = account_mapping.customer_id