{{ config(materialized='table') }}

WITH account_mapping AS (
    SELECT 
        kundennummer,
        '001' || SUBSTRING(MD5(kundennummer), 1, 15) AS account_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT 
    k.kontakt_id AS "Id",
    TRIM(k.rufname) AS "FirstName",
    TRIM(k.familienname) AS "LastName",
    TRIM(LOWER(k.kontakt_email)) AS "Email",
    TRIM(k.tel) AS "Phone",
    TRIM(k.berufsbezeichnung) AS "Title",
    CASE 
        WHEN TRIM(LOWER(k.rolle)) IN ('entscheidungsträger', 'decision maker') THEN 'Decision Maker'
        WHEN TRIM(LOWER(k.rolle)) IN ('endbenutzer', 'end user') THEN 'End User'
        WHEN TRIM(LOWER(k.rolle)) IN ('technischer kontakt', 'technical contact') THEN 'Technical Contact'
        WHEN TRIM(LOWER(k.rolle)) IN ('executive sponsor', 'geschäftsführung') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN TRIM(LOWER(k.korrespondenzsprache)) IN ('deutsch', 'de', 'german') THEN 'DE'
        WHEN TRIM(LOWER(k.korrespondenzsprache)) IN ('englisch', 'en', 'english') THEN 'EN'
        WHEN TRIM(LOWER(k.korrespondenzsprache)) IN ('französisch', 'fr', 'french') THEN 'FR'
        WHEN TRIM(LOWER(k.korrespondenzsprache)) IN ('spanisch', 'es', 'spanish') THEN 'ES'
        WHEN TRIM(LOWER(k.korrespondenzsprache)) IN ('italienisch', 'it', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    am.account_id AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN account_mapping am ON TRIM(k.kd_nummer) = TRIM(am.kundennummer)