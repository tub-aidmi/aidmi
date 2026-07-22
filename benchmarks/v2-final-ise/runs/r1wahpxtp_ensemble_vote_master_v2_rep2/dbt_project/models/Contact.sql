{{ config(materialized='table') }}

SELECT 
    k."kontakt_id" AS "Id",
    TRIM(INITCAP(k."rufname")) AS "FirstName",
    TRIM(INITCAP(k."familienname")) AS "LastName",
    LOWER(TRIM(k."kontakt_email")) AS "Email",
    k."tel" AS "Phone",
    TRIM(INITCAP(k."berufsbezeichnung")) AS "Title",
    CASE 
        WHEN UPPER(TRIM(k."rolle")) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(k."rolle")) IN ('END USER', 'ENDANWENDER', 'END USER') THEN 'End User'
        WHEN UPPER(TRIM(k."rolle")) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(k."rolle")) IN ('EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('SPANISH', 'SPANISCH') THEN 'ES'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('ITALIAN', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    c."kundennummer" AS "AccountId",
    k."kontakt_id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c 
    ON k."kd_nummer" = c."kundennummer"