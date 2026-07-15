{{ config(materialized='table') }}

SELECT
    MD5(kontakt_id) AS "Id",
    NULLIF(TRIM(rufname), '') AS "FirstName",
    COALESCE(NULLIF(TRIM(familienname), ''), 'Unknown') AS "LastName",
    NULLIF(TRIM(kontakt_email), '') AS "Email",
    NULLIF(TRIM(tel), '') AS "Phone",
    NULLIF(TRIM(berufsbezeichnung), '') AS "Title",
    CASE 
        WHEN LOWER(TRIM(rolle)) IN ('entscheidungsträger', 'decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(rolle)) IN ('endbenutzer', 'end user', 'benutzer') THEN 'End User'
        WHEN LOWER(TRIM(rolle)) IN ('technischer kontakt', 'technical contact', 'technik') THEN 'Technical Contact'
        WHEN LOWER(TRIM(rolle)) IN ('executive sponsor', 'geschäftsführung', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('FR', 'FRANZÖSISCH', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('ES', 'SPANISCH', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('IT', 'ITALIENISCH', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE 
        WHEN kd_nummer IS NOT NULL THEN MD5(kd_nummer)
        ELSE NULL
    END AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
