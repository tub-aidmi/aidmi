{{ config(materialized='table') }}

SELECT
    '003' || SUBSTRING(MD5(k.kontakt_id) FROM 1 FOR 15) AS "Id",
    NULLIF(TRIM(k.rufname), '') AS "FirstName",
    COALESCE(NULLIF(TRIM(k.familienname), ''), 'Unknown') AS "LastName",
    NULLIF(TRIM(k.kontakt_email), '') AS "Email",
    k.tel AS "Phone",
    NULLIF(TRIM(k.berufsbezeichnung), '') AS "Title",
    CASE 
        WHEN UPPER(TRIM(k.rolle)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(k.rolle)) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(k.rolle)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(k.rolle)) IN ('EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('DEUTSCH', 'DE') THEN 'DE'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ENGLISH', 'EN') THEN 'EN'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('FRENCH', 'FR') THEN 'FR'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('SPANISH', 'ES') THEN 'ES'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ITALIAN', 'IT') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || SUBSTRING(MD5(c.kundennummer) FROM 1 FOR 15) AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')::TEXT AS "CreatedDate",
    TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c
    ON k.kd_nummer = c.kundennummer
