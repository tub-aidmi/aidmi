{{ config(materialized='table') }}

SELECT
    'C' || TRIM(k.kontakt_id) AS "Id",

    k.rufname AS "FirstName",

    COALESCE(NULLIF(TRIM(k.familienname), ''), 'Unknown') AS "LastName",

    k.kontakt_email AS "Email",

    k.tel AS "Phone",

    k.berufsbezeichnung AS "Title",

    CASE
        WHEN LOWER(TRIM(COALESCE(k.rolle, ''))) IN ('entscheider', 'decision maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(COALESCE(k.rolle, ''))) IN ('endbenutzer', 'end user', 'anwender', 'nutzer') THEN 'End User'
        WHEN LOWER(TRIM(COALESCE(k.rolle, ''))) IN ('technischer ansprechpartner', 'technical contact', 'techniker', 'it-support') THEN 'Technical Contact'
        WHEN LOWER(TRIM(COALESCE(k.rolle, ''))) IN ('vorstand', 'gf', 'geschäftsführer', 'executive sponsor', 'ceo', 'cfo', 'cto') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",

    CASE
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('DE', 'DEU', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('EN', 'ENG', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('FR', 'FRA', 'FRANZÖSISCH', 'FRANÇAIS') THEN 'FR'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('ES', 'ESP', 'SPANISCH', 'ESPAÑOL') THEN 'ES'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('IT', 'ITA', 'ITALIENISCH', 'ITALIANO') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",

    CASE
        WHEN m.kundennummer IS NOT NULL THEN 'A' || TRIM(m.kundennummer)
        ELSE NULL
    END AS "AccountId",

    k.kontakt_id AS "Legacy_Contact_ID__c",

    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",

    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} m
    ON TRIM(k.kd_nummer) = TRIM(m.kundennummer)
WHERE k.kontakt_id IS NOT NULL
  AND TRIM(k.kontakt_id) != ''