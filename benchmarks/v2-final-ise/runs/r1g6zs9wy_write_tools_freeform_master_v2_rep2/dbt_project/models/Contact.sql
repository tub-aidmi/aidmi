{{ config(materialized='table') }}

SELECT 
    MD5(kontakt_id) AS "Id",
    TRIM(rufname) AS "FirstName",
    TRIM(familienname) AS "LastName",
    TRIM(kontakt_email) AS "Email",
    TRIM(tel) AS "Phone",
    TRIM(berufsbezeichnung) AS "Title",
    CASE 
        WHEN LOWER(TRIM(rolle)) IN ('entscheidungsträger', 'decision maker', 'entscheidungstraeger') THEN 'Decision Maker'
        WHEN LOWER(TRIM(rolle)) IN ('endbenutzer', 'end user', 'benutzer') THEN 'End User'
        WHEN LOWER(TRIM(rolle)) IN ('technischer kontakt', 'technical contact', 'technik') THEN 'Technical Contact'
        WHEN LOWER(TRIM(rolle)) IN ('geschäftsführung', 'executive sponsor', 'führung') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('deutsch', 'de', 'german') THEN 'DE'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('englisch', 'en', 'english') THEN 'EN'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('französisch', 'fr', 'french') THEN 'FR'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('spanisch', 'es', 'spanish') THEN 'ES'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('italienisch', 'it', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kd_nummer) AS "AccountId",
    TRIM(kontakt_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}