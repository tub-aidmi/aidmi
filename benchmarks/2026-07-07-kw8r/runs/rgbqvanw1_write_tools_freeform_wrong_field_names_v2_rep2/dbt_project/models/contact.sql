{{ config(materialized='table') }}

SELECT
    CONCAT('003', LEFT(MD5(ap_id), 17)) AS "Id",
    INITCAP(TRIM(vorname)) AS "FirstName",
    INITCAP(TRIM(nachname)) AS "LastName",
    LOWER(TRIM(email_adresse)) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    INITCAP(TRIM(position)) AS "Title",
    CASE
        WHEN UPPER(TRIM(funktion)) IN ('ENTSCHEIDUNGSBERECHTIGTER', 'DECISION MAKER', 'LEITUNG', 'GESCHÄFTSFÜHRER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(funktion)) IN ('VORSITZENDER', 'CEO', 'CFO', 'CTO', 'GM', 'GENERAL MANAGER') THEN 'Executive Sponsor'
        WHEN UPPER(TRIM(funktion)) IN ('TECHNISCHER KONTAKT', 'TECHNICAL CONTACT', 'INGENIEUR', 'IT') THEN 'Technical Contact'
        ELSE 'End User'
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(sprache)) IN ('DE', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(sprache)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(sprache)) IN ('FR', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(sprache)) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(sprache)) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CONCAT('001', LEFT(MD5(kunde), 17)) AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
