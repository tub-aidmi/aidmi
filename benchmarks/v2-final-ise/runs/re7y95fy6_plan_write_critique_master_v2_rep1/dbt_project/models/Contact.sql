{{ config(materialized='table') }}

SELECT 
    REGEXP_REPLACE(TRIM(UPPER(kontakt_id)), '^[A-Z]+', '', 'g') AS "Id",
    TRIM(INITCAP(rufname)) AS "FirstName",
    COALESCE(NULLIF(TRIM(familienname), ''), 'UNKNOWN') AS "LastName",
    LOWER(TRIM(kontakt_email)) AS "Email",
    REGEXP_REPLACE(tel, '[^0-9+]', '', 'g') AS "Phone",
    TRIM(INITCAP(berufsbezeichnung)) AS "Title",
    CASE UPPER(TRIM(rolle))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    REGEXP_REPLACE(TRIM(UPPER(kd_nummer)), '^[A-Z]+', '', 'g') AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    '2024-01-01 00:00:00' AS "CreatedDate",
    '2024-01-01 00:00:00' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}