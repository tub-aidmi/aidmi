{{ config(materialized='table') }}

SELECT
    '00Q' || LEFT(MD5('Contact_' || TRIM(UPPER(REGEXP_REPLACE(ap_id, '^[^a-zA-Z0-9]+', '')))), 15) AS "Id",
    INITCAP(TRIM(vorname)) AS "FirstName",
    COALESCE(NULLIF(INITCAP(TRIM(nachname)), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(email_adresse)) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    INITCAP(TRIM(position)) AS "Title",
    CASE UPPER(TRIM(funktion))
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'NUTZER' THEN 'End User'
        WHEN 'BENUTZER' THEN 'End User'
        WHEN 'TECHNISCH' THEN 'Technical Contact'
        WHEN 'SUPPORT' THEN 'Technical Contact'
        WHEN 'VORSTAND' THEN 'Executive Sponsor'
        WHEN 'C-LEVEL' THEN 'Executive Sponsor'
        WHEN 'CEO' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(sprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'GERMAN' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRENCH' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'SPANISH' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        WHEN 'ITALIAN' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || LEFT(MD5('Account_' || TRIM(UPPER(REGEXP_REPLACE(kunde, '^[^a-zA-Z0-9]+', '')))), 15) AS "AccountId",
    TRIM(ap_id) AS "Legacy_Contact_ID__c",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}