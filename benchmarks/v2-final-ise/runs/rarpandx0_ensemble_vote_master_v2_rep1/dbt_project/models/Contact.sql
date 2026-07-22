{{ config(materialized='table') }}

WITH contact_data AS (
    SELECT
        mk.kontakt_id,
        mk.rufname,
        mk.familienname,
        mk.kontakt_email,
        mk.tel,
        mk.berufsbezeichnung,
        mk.rolle,
        mk.korrespondenzsprache,
        mk.kd_nummer,
        mku.kundennummer AS account_kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} mk
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mku
        ON mk.kd_nummer = mku.kundennummer
)

SELECT
    kontakt_id AS "Id",
    NULLIF(TRIM(rufname), '') AS "FirstName",
    NULLIF(TRIM(familienname), '') AS "LastName",
    NULLIF(TRIM(kontakt_email), '') AS "Email",
    NULLIF(TRIM(tel), '') AS "Phone",
    NULLIF(TRIM(berufsbezeichnung), '') AS "Title",
    CASE
        WHEN LOWER(TRIM(rolle)) IN ('decision maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(rolle)) IN ('end user', 'endanwender', 'enduser', 'n/a') THEN 'End User'
        WHEN LOWER(TRIM(rolle)) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(rolle)) IN ('executive sponsor', 'sponsor', 'entscheider') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('de', 'deutsch', 'german', 'deutsch') THEN 'DE'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('fr', 'french', 'französisch') THEN 'FR'
        WHEN LOWER(TRIM(korrespondenzsprache)) = 'es' THEN 'ES'
        WHEN LOWER(TRIM(korrespondenzsprache)) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    account_kundennummer AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_data