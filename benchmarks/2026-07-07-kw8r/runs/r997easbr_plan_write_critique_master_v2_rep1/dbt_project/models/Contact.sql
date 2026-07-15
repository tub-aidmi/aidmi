{{ config(materialized='table') }}

SELECT
    '003' || UPPER(TRIM(kontakt_id)) AS "Id",
    INITCAP(TRIM(rufname)) AS "FirstName",
    COALESCE(NULLIF(INITCAP(TRIM(familienname)), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(kontakt_email)) AS "Email",
    CASE
        WHEN tel IS NULL OR REGEXP_REPLACE(TRIM(tel), '[^0-9+]', '', 'g') = '' THEN NULL
        ELSE REGEXP_REPLACE(TRIM(tel), '[^0-9+]', '', 'g')
    END AS "Phone",
    INITCAP(TRIM(berufsbezeichnung)) AS "Title",
    CASE
        WHEN LOWER(TRIM(COALESCE(rolle, ''))) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(COALESCE(rolle, ''))) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(COALESCE(rolle, ''))) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(COALESCE(rolle, ''))) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('fr', 'français', 'french', 'französisch') THEN 'FR'
        WHEN LOWER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('es', 'spanisch', 'spanish') THEN 'ES'
        WHEN LOWER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('it', 'italienisch', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    'A00' || UPPER(TRIM(kd_nummer)) AS "AccountId",
    TRIM(kontakt_id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}