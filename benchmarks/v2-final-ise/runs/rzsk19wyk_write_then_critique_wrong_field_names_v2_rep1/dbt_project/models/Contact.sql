{{ config(materialized='table') }}

WITH kunden_transformed AS (
    SELECT 
        kunden_nr,
        LOWER(SUBSTR(MD5('acc_' || kt.kunden_nr), 1, 15)) AS sf_account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} kt
)

SELECT
    ap.ap_id AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    COALESCE(NULLIF(INITCAP(TRIM(ap.nachname)), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    ap.telefonnummer AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE 
        WHEN INITCAP(TRIM(ap.funktion)) = 'Decision Maker' THEN 'Decision Maker'
        WHEN INITCAP(TRIM(ap.funktion)) = 'End User' THEN 'End User'
        WHEN INITCAP(TRIM(ap.funktion)) = 'Technical Contact' THEN 'Technical Contact'
        WHEN INITCAP(TRIM(ap.funktion)) = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(ap.sprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    kt.sf_account_id AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN kunden_transformed kt 
    ON TRIM(ap.kunde) = TRIM(kt.kunden_nr)