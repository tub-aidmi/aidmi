{{ config(materialized='table') }}

SELECT 
    'CONTACT-' || k."kontakt_id" AS "Id",
    k."rufname" AS "FirstName",
    k."familienname" AS "LastName",
    k."kontakt_email" AS "Email",
    k."tel" AS "Phone",
    k."berufsbezeichnung" AS "Title",
    CASE 
        WHEN UPPER(TRIM(k."rolle")) IN ('DECISION MAKER', 'ENTSCHEIDER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(k."rolle")) IN ('END USER', 'ENDANWENDER', 'END USER') THEN 'End User'
        WHEN UPPER(TRIM(k."rolle")) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNISCHER KONTAKT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(k."rolle")) IN ('EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('SPANISH', 'ESPANOL') THEN 'ES'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('ITALIAN', 'ITALIENISCH') THEN 'IT'
        WHEN UPPER(TRIM(k."korrespondenzsprache")) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(k."korrespondenzsprache"))
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE 
        WHEN c."kundennummer" IS NOT NULL THEN 'ACCT-' || c."kundennummer"
        ELSE NULL
    END AS "AccountId",
    k."kontakt_id" AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c 
    ON k."kd_nummer" = c."kundennummer"