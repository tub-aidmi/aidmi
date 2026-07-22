{{ config(materialized='table') }}

WITH contact_data AS (
    SELECT
        k.kontakt_id,
        k.rufname,
        k.familienname,
        k.kontakt_email,
        k.tel,
        k.berufsbezeichnung,
        k.rolle,
        k.korrespondenzsprache,
        k.kd_nummer,
        c.kundennummer AS account_kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c 
        ON k.kd_nummer = c.kundennummer
)

SELECT
    kontakt_id AS "Id",
    rufname AS "FirstName",
    familienname AS "LastName",
    kontakt_email AS "Email",
    tel AS "Phone",
    berufsbezeichnung AS "Title",
    CASE 
        WHEN UPPER(TRIM(rolle)) IN ('DECISION MAKER', 'DECISIONMAKER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(rolle)) IN ('END USER', 'ENDUSER') THEN 'End User'
        WHEN UPPER(TRIM(rolle)) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNICAL') THEN 'Technical Contact'
        WHEN UPPER(TRIM(rolle)) IN ('EXECUTIVE SPONSOR', 'EXECUTIVE') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('ES', 'SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('IT', 'ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    account_kundennummer AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_data
