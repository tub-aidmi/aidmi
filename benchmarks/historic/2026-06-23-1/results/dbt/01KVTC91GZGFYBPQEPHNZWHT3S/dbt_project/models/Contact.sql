{{ config(materialized='table') }}

SELECT
    kontakt_id AS Id,
    INITCAP(TRIM(rufname)) AS FirstName,
    INITCAP(TRIM(familienname)) AS LastName,
    LOWER(TRIM(kontakt_email)) AS Email,
    TRIM(tel) AS Phone,
    INITCAP(TRIM(berufsbezeichnung)) AS Title,
    CASE
        WHEN TRIM(rolle) IN ('Decision Maker', 'Entscheider') THEN 'Decision Maker'
        WHEN TRIM(rolle) IN ('End User', 'Benutzer') THEN 'End User'
        WHEN TRIM(rolle) IN ('Technical Contact', 'Technischer Kontakt') THEN 'Technical Contact'
        WHEN TRIM(rolle) IN ('Executive Sponsor', 'Exekutiver Sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS Role__c,
    CASE
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS Preferred_Language__c,
    kd_nummer AS AccountId,
    kontakt_id AS Legacy_Contact_ID__c,
    NULL::text AS CreatedDate,
    NULL::text AS LastModifiedDate,
    0 AS IsDeleted
FROM {{ source('fixture_master_src', 'master_kontakte') }}