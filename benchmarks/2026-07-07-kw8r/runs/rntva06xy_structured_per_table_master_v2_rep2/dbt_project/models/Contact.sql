{{ config(materialized='table') }}

SELECT 
    mk."kontakt_id" AS "Id",
    mk."rufname" AS "FirstName",
    mk."familienname" AS "LastName",
    mk."kontakt_email" AS "Email",
    mk."tel" AS "Phone",
    mk."berufsbezeichnung" AS "Title",
    CASE 
        WHEN UPPER(TRIM(mk."rolle")) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(mk."rolle")) IN ('END USER', 'ENDANWENDER', 'ENDANWENDERIN') THEN 'End User'
        WHEN UPPER(TRIM(mk."rolle")) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNISCHE ANSPRECHPARTNERIN') THEN 'Technical Contact'
        WHEN UPPER(TRIM(mk."rolle")) IN ('EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(mk."korrespondenzsprache")) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(mk."korrespondenzsprache")) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(mk."korrespondenzsprache")) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(mk."korrespondenzsprache")) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(mk."korrespondenzsprache")) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    mk."kd_nummer" AS "AccountId",
    mk."kontakt_id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} mk