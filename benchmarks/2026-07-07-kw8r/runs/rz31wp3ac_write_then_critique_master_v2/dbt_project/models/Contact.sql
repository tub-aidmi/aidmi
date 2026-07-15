{{ config(materialized='table') }}
SELECT
    'CON-' || mk."kontakt_id" AS "Id",
    TRIM(mk."rufname") AS "FirstName",
    TRIM(mk."familienname") AS "LastName",
    TRIM(LOWER(mk."kontakt_email")) AS "Email",
    TRIM(mk."tel") AS "Phone",
    TRIM(mk."berufsbezeichnung") AS "Title",
    CASE 
        WHEN TRIM(LOWER(mk."rolle")) IN ('entscheidungsträger', 'decision maker') THEN 'Decision Maker'
        WHEN TRIM(LOWER(mk."rolle")) IN ('endbenutzer', 'end user') THEN 'End User'
        WHEN TRIM(LOWER(mk."rolle")) IN ('technischer kontakt', 'technical contact') THEN 'Technical Contact'
        WHEN TRIM(LOWER(mk."rolle")) IN ('executive sponsor', 'geschäftsführung') THEN 'Executive Sponsor'
        ELSE NULL 
    END AS "Role__c",
    CASE 
        WHEN TRIM(UPPER(mk."korrespondenzsprache")) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN TRIM(UPPER(mk."korrespondenzsprache")) IN ('EN', 'ENGLISH') THEN 'EN'
        WHEN TRIM(UPPER(mk."korrespondenzsprache")) IN ('FR', 'FRANZÖSISCH') THEN 'FR'
        WHEN TRIM(UPPER(mk."korrespondenzsprache")) IN ('ES', 'SPANISCH') THEN 'ES'
        WHEN TRIM(UPPER(mk."korrespondenzsprache")) IN ('IT', 'ITALIENISCH') THEN 'IT'
        ELSE NULL 
    END AS "Preferred_Language__c",
    'ACC-' || mk2."kundennummer" AS "AccountId",
    mk."kontakt_id" AS "Legacy_Contact_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} mk
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk2 ON mk."kd_nummer" = mk2."kundennummer"