{{ config(materialized='table') }}

WITH account_map AS (
    SELECT 
        TRIM(kunden_nr) AS account_id,
        kunden_nr AS legacy_customer_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
)

SELECT 
    TRIM(a.ap_id) AS "Id",
    TRIM(a.vorname) AS "FirstName",
    COALESCE(NULLIF(TRIM(a.nachname), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(a.email_adresse)) AS "Email",
    TRIM(a.telefonnummer) AS "Phone",
    INITCAP(TRIM(a.position)) AS "Title",
    CASE 
        WHEN LOWER(TRIM(a.funktion)) IN ('entscheider', 'decision maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(a.funktion)) IN ('endanwender', 'end user') THEN 'End User'
        WHEN LOWER(TRIM(a.funktion)) IN ('techniker', 'technical contact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(a.funktion)) IN ('geschäftsführer', 'executive sponsor', 'business owner') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN LOWER(TRIM(a.sprache)) IN ('de', 'deutsch', 'deu') THEN 'DE'
        WHEN LOWER(TRIM(a.sprache)) IN ('en', 'englisch', 'eng') THEN 'EN'
        WHEN LOWER(TRIM(a.sprache)) IN ('fr', 'französisch', 'fra') THEN 'FR'
        WHEN LOWER(TRIM(a.sprache)) IN ('es', 'spanisch', 'esp') THEN 'ES'
        WHEN LOWER(TRIM(a.sprache)) IN ('it', 'italienisch', 'ita') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    am.account_id AS "AccountId",
    TRIM(a.ap_id) AS "Legacy_Contact_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} a
LEFT JOIN account_map am ON TRIM(a.kunde) = am.account_id