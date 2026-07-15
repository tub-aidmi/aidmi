{{ config(materialized='table') }}

SELECT
    REPLACE(kontakt_id, 'CON-', '') AS "Id",
    INITCAP(TRIM(rufname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(familienname)), 'Unknown') AS "LastName",
    TRIM(kontakt_email) AS "Email",
    TRIM(tel) AS "Phone",
    INITCAP(TRIM(berufsbezeichnung)) AS "Title",
    CASE
        WHEN LOWER(TRIM(rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(rolle)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(rolle)) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(rolle)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('deutsch', 'de', 'german') THEN 'DE'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('english', 'englisch', 'en') THEN 'EN'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('french', 'französisch', 'fr') THEN 'FR'
        WHEN LOWER(TRIM(korrespondenzsprache)) = 'spanish' THEN 'ES'
        WHEN LOWER(TRIM(korrespondenzsprache)) = 'italian' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || LPAD(regexp_replace(regexp_replace(kd_nummer, 'CUST-', ''), '[^0-9]', ''), 9, '0') AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}