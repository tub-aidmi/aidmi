{{ config(materialized='table') }}

SELECT
    MD5(TRIM(kontakt_id)) AS "Id",
    TRIM(rufname) AS "FirstName",
    COALESCE(TRIM(familienname), TRIM(kontakt_id)) AS "LastName",
    TRIM(kontakt_email) AS "Email",
    TRIM(tel) AS "Phone",
    TRIM(berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(rolle)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(rolle)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(rolle)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(rolle)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(korrespondenzsprache)) = 'de' THEN 'DE'
        WHEN LOWER(TRIM(korrespondenzsprache)) = 'en' THEN 'EN'
        WHEN LOWER(TRIM(korrespondenzsprache)) = 'fr' THEN 'FR'
        WHEN LOWER(TRIM(korrespondenzsprache)) = 'es' THEN 'ES'
        WHEN LOWER(TRIM(korrespondenzsprache)) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(TRIM(kd_nummer)) AS "AccountId",
    TRIM(kontakt_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }}
