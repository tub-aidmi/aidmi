{{ config(materialized='table') }}

WITH account_mapping AS (
    SELECT 
        kundennummer,
        'ACCT-' || LPAD(ROW_NUMBER() OVER (ORDER BY kundennummer)::TEXT, 6, '0') AS account_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
    'CONT-' || LPAD(ROW_NUMBER() OVER (ORDER BY kt.kontakt_id)::TEXT, 6, '0') AS "Id",
    kt.rufname AS "FirstName",
    kt.familienname AS "LastName",
    kt.kontakt_email AS "Email",
    kt.tel AS "Phone",
    kt.berufsbezeichnung AS "Title",
    CASE 
        WHEN UPPER(TRIM(kt.rolle)) IN ('DECISION MAKER', 'END USER', 'TECHNICAL CONTACT', 'EXECUTIVE SPONSOR') 
            THEN INITCAP(LOWER(TRIM(kt.rolle)))
        WHEN UPPER(TRIM(kt.rolle)) IN ('TECHNISCHER ANSPRECHPARTNER', 'TECHNISCHER KONTAKT') 
            THEN 'Technical Contact'
        WHEN UPPER(TRIM(kt.rolle)) IN ('ENTSCHEIDER') 
            THEN 'Decision Maker'
        WHEN UPPER(TRIM(kt.rolle)) IN ('ENDANWENDER') 
            THEN 'End User'
        WHEN UPPER(TRIM(kt.rolle)) IN ('SPONSOR') 
            THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(kt.korrespondenzsprache)) IN ('DE', 'DEUTSCH', 'GERMAN') 
            THEN 'DE'
        WHEN UPPER(TRIM(kt.korrespondenzsprache)) IN ('EN', 'ENGLISH', 'ENGLISCH') 
            THEN 'EN'
        WHEN UPPER(TRIM(kt.korrespondenzsprache)) IN ('FR', 'FRANZÖSISCH', 'FRENCH', 'FRANÇAIS') 
            THEN 'FR'
        WHEN UPPER(TRIM(kt.korrespondenzsprache)) IN ('ES', 'SPANISH') 
            THEN 'ES'
        WHEN UPPER(TRIM(kt.korrespondenzsprache)) IN ('IT', 'ITALIAN') 
            THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    am.account_id AS "AccountId",
    kt.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} kt
LEFT JOIN account_mapping am ON kt.kd_nummer = am.kundennummer
