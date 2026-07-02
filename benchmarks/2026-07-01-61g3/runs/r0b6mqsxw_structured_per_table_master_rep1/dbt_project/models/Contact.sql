{{ config(materialized='table') }}

SELECT
    CAST(kontakt_id AS TEXT) AS "Id",
    TRIM(INITCAP(rufname)) AS "FirstName",
    COALESCE(TRIM(familienname), 'Unknown') AS "LastName",
    TRIM(kontakt_email) AS "Email",
    TRIM(tel) AS "Phone",
    TRIM(INITCAP(berufsbezeichnung)) AS "Title",
    CASE
        WHEN LOWER(TRIM(rolle)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(rolle)) = 'entscheider' THEN 'Decision Maker'
        WHEN LOWER(TRIM(rolle)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(rolle)) = 'technical contact' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('de', 'deutsch') THEN 'DE'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('fr') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(kd_nummer) AS "AccountId",
    CAST(kontakt_id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_kontakte') }}