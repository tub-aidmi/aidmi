{{ config(materialized='table') }}

SELECT
    '003' || REPLACE(k.kontakt_id, 'CON-', '') AS "Id",
    NULLIF(TRIM(k.rufname), '') AS "FirstName",
    COALESCE(NULLIF(TRIM(k.familienname), ''), 'Unknown') AS "LastName",
    NULLIF(TRIM(k.kontakt_email), '') AS "Email",
    NULLIF(TRIM(k.tel), '') AS "Phone",
    NULLIF(TRIM(k.berufsbezeichnung), '') AS "Title",
    CASE 
        WHEN UPPER(TRIM(k.rolle)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNISCHER KONTAKT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(k.rolle)) IN ('END USER', 'ENDNUTZER') THEN 'End User'
        WHEN UPPER(TRIM(k.rolle)) IN ('DECISION MAKER', 'ENTSCHEIDUNGSTRÄGER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(k.rolle)) IN ('EXECUTIVE SPONSOR', 'EXECUTIVE') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE 
        WHEN k.kd_nummer IS NOT NULL THEN '001' || REPLACE(k.kd_nummer, 'CUST-M', '')
        ELSE NULL
    END AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
