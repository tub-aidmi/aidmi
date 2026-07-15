{{ config(materialized='table') }}
SELECT 
    "kontakt_id" AS "Id",
    TRIM("rufname") AS "FirstName",
    TRIM("familienname") AS "LastName",
    TRIM(LOWER("kontakt_email")) AS "Email",
    TRIM("tel") AS "Phone",
    TRIM("berufsbezeichnung") AS "Title",
    CASE 
        WHEN TRIM(LOWER("rolle")) IN ('entscheidungsträger', 'decision maker') THEN 'Decision Maker'
        WHEN TRIM(LOWER("rolle")) IN ('endbenutzer', 'end user') THEN 'End User'
        WHEN TRIM(LOWER("rolle")) IN ('technischer kontakt', 'technical contact') THEN 'Technical Contact'
        WHEN TRIM(LOWER("rolle")) IN ('executive sponsor', 'geschäftsführung') THEN 'Executive Sponsor'
        ELSE NULL 
    END AS "Role__c",
    CASE 
        WHEN TRIM(LOWER("korrespondenzsprache")) IN ('deutsch', 'de') THEN 'DE'
        WHEN TRIM(LOWER("korrespondenzsprache")) IN ('englisch', 'en') THEN 'EN'
        WHEN TRIM(LOWER("korrespondenzsprache")) IN ('französisch', 'fr') THEN 'FR'
        WHEN TRIM(LOWER("korrespondenzsprache")) IN ('spanisch', 'es') THEN 'ES'
        WHEN TRIM(LOWER("korrespondenzsprache")) IN ('italienisch', 'it') THEN 'IT'
        ELSE NULL 
    END AS "Preferred_Language__c",
    (
        SELECT MD5("kundennummer")
        FROM {{ source('fixture_master_v2_src', 'master_kunden') }} mk
        WHERE mk."kundennummer" = mk2."kd_nummer"
    ) AS "AccountId",
    "kontakt_id" AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} mk2