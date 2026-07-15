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
        WHEN TRIM(LOWER("rolle")) IN ('executive sponsor', 'geschäftsführer') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(LOWER("korrespondenzsprache")) IN ('deutsch', 'de', 'german') THEN 'DE'
        WHEN TRIM(LOWER("korrespondenzsprache")) IN ('englisch', 'en', 'english') THEN 'EN'
        WHEN TRIM(LOWER("korrespondenzsprache")) IN ('französisch', 'fr', 'french') THEN 'FR'
        WHEN TRIM(LOWER("korrespondenzsprache")) IN ('spanisch', 'es', 'spanish') THEN 'ES'
        WHEN TRIM(LOWER("korrespondenzsprache")) IN ('italienisch', 'it', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    COALESCE(
        (SELECT "kundennummer" FROM {{ source('fixture_master_v2_src', 'master_kunden') }} WHERE "kundennummer" = TRIM("kd_nummer")),
        (SELECT "kundennummer" FROM {{ source('fixture_master_v2_src', 'master_kunden') }} WHERE "kundennummer" = "kd_nummer")
    ) AS "AccountId",
    "kontakt_id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
