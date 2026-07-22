{{ config(materialized='table') }}

WITH kontakte AS (
    SELECT
        kontakt_id,
        rufname,
        familienname,
        kontakt_email,
        tel,
        berufsbezeichnung,
        rolle,
        korrespondenzsprache,
        kd_nummer
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
),

kunden AS (
    SELECT
        kundennummer,
        '001' || ENCODE(DIGEST(kundennummer, 'md5'), 'hex') AS account_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

contact_mapping AS (
    SELECT
        '003' || ENCODE(DIGEST(k.kontakt_id, 'md5'), 'hex') AS Id,
        NULLIF(TRIM(k.rufname), '') AS FirstName,
        INITCAP(TRIM(k.familienname)) AS LastName,
        NULLIF(TRIM(k.kontakt_email), '') AS Email,
        NULLIF(TRIM(k.tel), '') AS Phone,
        NULLIF(TRIM(k.berufsbezeichnung), '') AS Title,
        CASE 
            WHEN UPPER(TRIM(k.rolle)) IN ('ENTSCHEIDUNGSTRÄGER', 'ENTSCHEIDUNGSTRAGER') THEN 'Decision Maker'
            WHEN UPPER(TRIM(k.rolle)) IN ('ENDNUTZER', 'END USER') THEN 'End User'
            WHEN UPPER(TRIM(k.rolle)) IN ('TECHNISCHER KONTAKT', 'TECHNICAL CONTACT') THEN 'Technical Contact'
            WHEN UPPER(TRIM(k.rolle)) IN ('EXECUTIVE SPONSOR', 'GESCHÄFTSFÜHRER') THEN 'Executive Sponsor'
            ELSE NULL
        END AS "Role__c",
        CASE 
            WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('DEUTSCH', 'DE') THEN 'DE'
            WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ENGLISCH', 'EN') THEN 'EN'
            WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('FRANZÖSISCH', 'FR') THEN 'FR'
            WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('SPANISCH', 'ES') THEN 'ES'
            WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ITALIENISCH', 'IT') THEN 'IT'
            ELSE NULL
        END AS "Preferred_Language__c",
        kd.account_id AS AccountId,
        k.kontakt_id AS Legacy_Contact_ID__c,
        TO_CHAR(NOW(), 'YYYY-MM-DD') AS CreatedDate,
        TO_CHAR(NOW(), 'YYYY-MM-DD') AS LastModifiedDate,
        0 AS IsDeleted
    FROM kontakte k
    LEFT JOIN kunden kd ON k.kd_nummer = kd.kundennummer
)

SELECT
    Id,
    FirstName,
    LastName,
    Email,
    Phone,
    Title,
    "Role__c",
    "Preferred_Language__c",
    AccountId,
    Legacy_Contact_ID__c,
    CreatedDate,
    LastModifiedDate,
    IsDeleted
FROM contact_mapping
