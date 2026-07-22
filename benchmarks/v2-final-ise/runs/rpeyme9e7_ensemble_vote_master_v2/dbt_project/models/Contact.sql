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
        WHEN UPPER(TRIM(mk."rolle")) IN ('END USER', 'ENDANWENDER', 'END USER') THEN 'End User'
        WHEN UPPER(TRIM(mk."rolle")) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER') THEN 'Technical Contact'
        WHEN UPPER(TRIM(mk."rolle")) IN ('EXECUTIVE SPONSOR', 'SPONSOR') THEN 'Executive Sponsor'
        WHEN mk."rolle" IS NULL OR TRIM(mk."rolle") = '' OR UPPER(TRIM(mk."rolle")) = 'N/A' THEN NULL
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(mk."korrespondenzsprache")) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(mk."korrespondenzsprache")) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(mk."korrespondenzsprache")) IN ('FR', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(mk."korrespondenzsprache")) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(mk."korrespondenzsprache")) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    mku."kundennummer" AS "AccountId",
    mk."kontakt_id" AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} mk
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mku
    ON mk."kd_nummer" = mku."kundennummer"