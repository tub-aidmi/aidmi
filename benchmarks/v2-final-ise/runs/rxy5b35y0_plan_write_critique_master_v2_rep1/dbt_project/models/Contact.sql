{{ config(materialized='table') }}
SELECT
    MD5(k.kontakt_id) AS "Id",
    INITCAP(TRIM(k.rufname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(k.familienname)), 'Unknown') AS "LastName",
    TRIM(k.kontakt_email) AS "Email",
    TRIM(k.tel) AS "Phone",
    TRIM(k.berufsbezeichnung) AS "Title",
    CASE 
        WHEN UPPER(TRIM(k.rolle)) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(k.rolle)) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(k.rolle)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNIKER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(k.rolle)) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    COALESCE(MD5(c.kundennummer), MD5('ORPHAN')) AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c 
    ON TRIM(k.kd_nummer) = TRIM(c.kundennummer)