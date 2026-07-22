{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id) AS "Id",
    TRIM(kontakte.rufname) AS "FirstName",
    COALESCE(TRIM(kontakte.familienname), 'Unknown') AS "LastName",
    LOWER(TRIM(kontakte.kontakt_email)) AS "Email",
    TRIM(kontakte.tel) AS "Phone",
    TRIM(kontakte.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(kontakte.rolle)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(kontakte.rolle)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(kontakte.rolle)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(kontakte.rolle)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(kontakte.korrespondenzsprache)) = 'de' THEN 'DE'
        WHEN LOWER(TRIM(kontakte.korrespondenzsprache)) = 'en' THEN 'EN'
        WHEN LOWER(TRIM(kontakte.korrespondenzsprache)) = 'fr' THEN 'FR'
        WHEN LOWER(TRIM(kontakte.korrespondenzsprache)) = 'es' THEN 'ES'
        WHEN LOWER(TRIM(kontakte.korrespondenzsprache)) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kontakte.kd_nummer) AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte