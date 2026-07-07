{{ config(materialized='table') }}

SELECT
    kontakt_id AS "Id",
    TRIM(rufname) AS "FirstName",
    COALESCE(TRIM(familienname), 'Unknown') AS "LastName", -- LastName is NOT NULL
    TRIM(kontakt_email) AS "Email",
    TRIM(tel) AS "Phone",
    TRIM(berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(rolle)) IN ('end user', 'endnutzer') THEN 'End User'
        WHEN LOWER(TRIM(rolle)) IN ('technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(rolle)) IN ('executive sponsor', 'führungskraft') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('fr', 'french', 'französisch') THEN 'FR'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('es', 'spanish', 'spanisch') THEN 'ES'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('it', 'italian', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kd_nummer AS "AccountId", -- Maps to Account.Id
    kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
