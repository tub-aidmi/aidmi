{{ config(materialized='table') }}

SELECT
    -- Contact Id: derived from kontakt_id with 'C' prefix for Salesforce-style format
    'C' || TRIM(k.kontakt_id) AS "Id",

    -- First Name (source field rufname = first name)
    k.rufname AS "FirstName",

    -- Last Name NOT NULL — default to 'Unknown' if source is null/blank
    COALESCE(NULLIF(TRIM(k.familienname), ''), 'Unknown') AS "LastName",

    -- Email address
    k.kontakt_email AS "Email",

    -- Phone number
    k.tel AS "Phone",

    -- Job title / profession (berufsbezeichnung)
    k.berufsbezeichnung AS "Title",

    -- Role__c: map source rolle to target enum values
    CASE
        WHEN LOWER(TRIM(COALESCE(k.rolle, ''))) IN ('entscheider', 'decision maker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(COALESCE(k.rolle, ''))) IN ('endbenutzer', 'end user', 'anwender', 'nutzer') THEN 'End User'
        WHEN LOWER(TRIM(COALESCE(k.rolle, ''))) IN ('technischer ansprechpartner', 'technical contact', 'techniker', 'it-support') THEN 'Technical Contact'
        WHEN LOWER(TRIM(COALESCE(k.rolle, ''))) IN ('vorstand', 'gf', 'geschäftsführer', 'executive sponsor', 'ceo', 'cfo', 'cto') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",

    -- Preferred_Language__c: map source korrespondenzsprache to target ISO codes
    CASE
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('DE', 'DEU', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('EN', 'ENG', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('FR', 'FRA', 'FRANZÖSISCH', 'FRANÇAIS') THEN 'FR'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('ES', 'ESP', 'SPANISCH', 'ESPAÑOL') THEN 'ES'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('IT', 'ITA', 'ITALIENISCH', 'ITALIANO') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",

    -- AccountId: transform kd_nummer with 'A' prefix to match Account.Id convention
    CASE
        WHEN m.kundennummer IS NOT NULL THEN 'A' || TRIM(m.kundennummer)
        ELSE NULL
    END AS "AccountId",

    -- Legacy_Contact_ID__c: source natural key for row-level verification
    k.kontakt_id AS "Legacy_Contact_ID__c",

    -- Audit fields not present in source master_kontakte — use sensible defaults
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",

    -- Soft-delete flag, no equivalent in source
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} m
    ON TRIM(k.kd_nummer) = TRIM(m.kundennummer)
WHERE k.kontakt_id IS NOT NULL
  AND TRIM(k.kontakt_id) != '';