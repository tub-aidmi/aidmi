{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        k.kontakt_id,
        k.rufname,
        k.familienname,
        k.kontakt_email,
        k.tel,
        k.berufsbezeichnung,
        k.rolle,
        k.korrespondenzsprache,
        k.kd_nummer,
        a.kundennummer AS account_kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} a
        ON k.kd_nummer = a.kundennummer
),

normalized AS (
    SELECT
        kontakt_id,
        INITCAP(TRIM(rufname)) AS first_name,
        INITCAP(TRIM(familienname)) AS last_name,
        LOWER(TRIM(kontakt_email)) AS email,
        TRIM(tel) AS phone,
        INITCAP(TRIM(berufsbezeichnung)) AS title,
        TRIM(rolle) AS role,
        TRIM(korrespondenzsprache) AS preferred_language,
        account_kundennummer
    FROM source_data
),

role_mapped AS (
    SELECT
        kontakt_id,
        first_name,
        last_name,
        email,
        phone,
        title,
        CASE
            WHEN LOWER(role) IN ('entscheidungsträger', 'decision maker') THEN 'Decision Maker'
            WHEN LOWER(role) IN ('endbenutzer', 'end user') THEN 'End User'
            WHEN LOWER(role) IN ('technischer kontakt', 'technical contact') THEN 'Technical Contact'
            WHEN LOWER(role) IN ('exekutiver sponsor', 'executive sponsor') THEN 'Executive Sponsor'
            ELSE NULL
        END AS role__c,
        preferred_language,
        account_kundennummer
    FROM normalized
),

language_mapped AS (
    SELECT
        kontakt_id,
        first_name,
        last_name,
        email,
        phone,
        title,
        role__c,
        CASE
            WHEN UPPER(preferred_language) IN ('DE', 'DEUTSCH') THEN 'DE'
            WHEN UPPER(preferred_language) IN ('EN', 'ENGLISH') THEN 'EN'
            WHEN UPPER(preferred_language) IN ('FR', 'FRANZÖSISCH', 'FRENCH') THEN 'FR'
            WHEN UPPER(preferred_language) IN ('ES', 'SPANISCH', 'SPANISH') THEN 'ES'
            WHEN UPPER(preferred_language) IN ('IT', 'ITALIENISCH', 'ITALIAN') THEN 'IT'
            ELSE NULL
        END AS preferred_language__c,
        account_kundennummer
    FROM role_mapped
)

SELECT
    MD5(kontakt_id || '_CONTACT') AS "Id",
    first_name AS "FirstName",
    last_name AS "LastName",
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
    role__c AS "Role__c",
    preferred_language__c AS "Preferred_Language__c",
    CASE
        WHEN account_kundennummer IS NOT NULL
        THEN MD5(account_kundennummer || '_ACCOUNT')
        ELSE NULL
    END AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM language_mapped
