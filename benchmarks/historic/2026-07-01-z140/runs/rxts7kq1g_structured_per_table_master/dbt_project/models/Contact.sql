{{ config(materialized='table') }}

SELECT
    CAST(k.kontakt_id AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE(k.rufname, ''))) AS "FirstName",
    INITCAP(TRIM(COALESCE(k.familienname, ''))) AS "LastName",
    LOWER(TRIM(COALESCE(k.kontakt_email, ''))) AS "Email",
    TRIM(COALESCE(k.tel, '')) AS "Phone",
    INITCAP(TRIM(COALESCE(k.berufsbezeichnung, ''))) AS "Title",
    CASE 
        WHEN UPPER(TRIM(COALESCE(k.rolle, ''))) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(COALESCE(k.rolle, ''))) = 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(COALESCE(k.rolle, ''))) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(COALESCE(k.rolle, ''))) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('EN', 'ENGLISCH', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('ES', 'ESPANOL', 'ESPAÑOL') THEN 'ES'
        WHEN UPPER(TRIM(COALESCE(k.korrespondenzsprache, ''))) IN ('IT', 'ITALIANO') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CAST(k.kd_nummer AS TEXT) AS "AccountId",
    CAST(k.kontakt_id AS TEXT) AS "Legacy_Contact_ID__c",
    CAST(NOW() AS TEXT) AS "CreatedDate",
    CAST(NOW() AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_src', 'master_kontakte') }} k