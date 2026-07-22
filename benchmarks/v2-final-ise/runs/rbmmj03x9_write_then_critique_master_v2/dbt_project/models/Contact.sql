{{ config(materialized='table') }}
SELECT 
    k."kontakt_id" AS "Id",
    TRIM(k."rufname") AS "FirstName",
    TRIM(k."familienname") AS "LastName",
    LOWER(TRIM(k."kontakt_email")) AS "Email",
    TRIM(k."tel") AS "Phone",
    TRIM(k."berufsbezeichnung") AS "Title",
    CASE 
        WHEN UPPER(TRIM(k."rolle")) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(k."rolle")) IN ('END USER', 'ENDANWENDER') THEN 'End User'
        WHEN UPPER(TRIM(k."rolle")) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(k."rolle")) IN ('EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL 
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('ITALIAN', 'ITALIENISCH') THEN 'IT'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(k."korrespondenzsprache"))
        ELSE NULL 
    END AS "Preferred_Language__c",
    CASE 
        WHEN k."kd_nummer" LIKE 'CUST-%' THEN CONCAT('ACCOUNT-', REPLACE(k."kd_nummer", 'CUST-', ''))
        ELSE NULL 
    END AS "AccountId",
    k."kontakt_id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k