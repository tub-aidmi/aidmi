{{ config(materialized='table') }}

SELECT
    k.kontakt_id AS "Id",
    INITCAP(TRIM(k.rufname)) AS "FirstName",
    COALESCE(NULLIF(TRIM(k.familienname), ''), '') AS "LastName",
    LOWER(TRIM(k.kontakt_email)) AS "Email",
    TRIM(k.tel) AS "Phone",
    INITCAP(TRIM(k.berufsbezeichnung)) AS "Title",
    CASE
        WHEN UPPER(TRIM(k.rolle)) IN ('DECISION MAKER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(k.rolle)) IN ('END USER') THEN 'End User'
        WHEN UPPER(TRIM(k.rolle)) IN ('TECHNICAL CONTACT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(k.rolle)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN UPPER(TRIM(k.rolle)) = 'ENTSCHEIDER' THEN 'Decision Maker'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('FR') THEN 'FR'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunden.kundennummer AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_src', 'master_kunden') }} kunden
    ON TRIM(k.kd_nummer) = TRIM(kunden.kundennummer)