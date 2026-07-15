{{ config(materialized='table') }}

WITH contact_data AS (
    SELECT 
        ap.ap_id,
        TRIM(INITCAP(ap.vorname)) AS first_name,
        TRIM(INITCAP(COALESCE(ap.nachname, ap.ap_id))) AS last_name,
        TRIM(LOWER(ap.email_adresse)) AS email,
        TRIM(ap.telefonnummer) AS phone,
        TRIM(ap.position) AS title,
        ap.funktion,
        ap.sprache,
        ap.kunde AS customer_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
)

SELECT 
    gen_random_uuid()::TEXT AS "Id",
    first_name AS "FirstName",
    last_name AS "LastName",
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
    CASE 
        WHEN funktion = 'Decision Maker' THEN 'Decision Maker'
        WHEN funktion = 'End User' THEN 'End User'
        WHEN funktion = 'Technical Contact' THEN 'Technical Contact'
        WHEN funktion = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN sprache = 'Deutsch' THEN 'DE'
        WHEN sprache = 'Englisch' THEN 'EN'
        WHEN sprache = 'Französisch' THEN 'FR'
        WHEN sprache = 'Spanisch' THEN 'ES'
        WHEN sprache = 'Italienisch' THEN 'IT'
        WHEN sprache IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN sprache
        ELSE NULL
    END AS "Preferred_Language__c",
    customer_id AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_data