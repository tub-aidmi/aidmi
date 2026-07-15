{{ config(materialized='table') }}

SELECT
    -- Salesforce-style Contact Id: prefix with 'K' for cross-table FK consistency
    'K' || TRIM("kontakt_id") AS "Id",
    TRIM("rufname") AS "FirstName",
    COALESCE(TRIM("familienname"), 'Unknown') AS "LastName",
    LOWER(TRIM("kontakt_email")) AS "Email",
    -- Clean phone: keep digits and leading +
    REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM("tel")), '[^0-9+]', '', 'g'), '^0', '+49 ', 'g') AS "Phone",
    INITCAP(TRIM("berufsbezeichnung")) AS "Title",
    -- Map role to target enum values, using uppercase comparison
    CASE
        WHEN UPPER(TRIM("rolle")) = 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN UPPER(TRIM("rolle")) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM("rolle")) = 'ENDANWENDER' THEN 'End User'
        WHEN UPPER(TRIM("rolle")) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM("rolle")) = 'TECHNISCHER KONTAKT' THEN 'Technical Contact'
        WHEN UPPER(TRIM("rolle")) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM("rolle")) = 'GESCHÄFTSFÜHRER' THEN 'Executive Sponsor'
        WHEN UPPER(TRIM("rolle")) = 'VORSTAND' THEN 'Executive Sponsor'
        WHEN UPPER(TRIM("rolle")) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    -- Map language to target enum (DE, EN, FR, ES, IT)
    CASE
        WHEN UPPER(TRIM("korrespondenzsprache")) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM("korrespondenzsprache")) = 'GERMAN' THEN 'DE'
        WHEN UPPER(TRIM("korrespondenzsprache")) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM("korrespondenzsprache")) = 'ENGLISH' THEN 'EN'
        WHEN UPPER(TRIM("korrespondenzsprache")) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM("korrespondenzsprache")) = 'FRENCH' THEN 'FR'
        WHEN UPPER(TRIM("korrespondenzsprache")) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM("korrespondenzsprache")) = 'SPANISH' THEN 'ES'
        WHEN UPPER(TRIM("korrespondenzsprache")) = 'IT' THEN 'IT'
        WHEN UPPER(TRIM("korrespondenzsprache")) = 'ITALIAN' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    -- AccountId: Salesforce-style, match Account.Id = 'C' || kundennummer
    CASE
        WHEN TRIM("kd_nummer") IS NOT NULL THEN 'C' || TRIM("kd_nummer")
        ELSE NULL
    END AS "AccountId",
    -- Legacy key from source natural key
    TRIM("kontakt_id") AS "Legacy_Contact_ID__c",
    -- Fixed dates
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
