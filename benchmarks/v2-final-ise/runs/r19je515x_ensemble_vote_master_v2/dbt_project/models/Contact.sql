{{ config(materialized='table') }}

SELECT 
    c."kontakt_id" AS "Id",
    INITCAP(TRIM(c."rufname")) AS "FirstName",
    INITCAP(TRIM(c."familienname")) AS "LastName",
    LOWER(TRIM(c."kontakt_email")) AS "Email",
    TRIM(c."tel") AS "Phone",
    INITCAP(TRIM(c."berufsbezeichnung")) AS "Title",
    CASE 
        WHEN LOWER(TRIM(c."rolle")) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(c."rolle")) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(c."rolle")) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(c."rolle")) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN LOWER(TRIM(c."korrespondenzsprache")) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(c."korrespondenzsprache")) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(TRIM(c."korrespondenzsprache")) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(TRIM(c."korrespondenzsprache")) IN ('es', 'spanisch', 'spanish') THEN 'ES'
        WHEN LOWER(TRIM(c."korrespondenzsprache")) IN ('it', 'italienisch', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    k."kundennummer" AS "AccountId",
    c."kontakt_id" AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} c
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k 
    ON TRIM(c."kd_nummer") = TRIM(k."kundennummer")