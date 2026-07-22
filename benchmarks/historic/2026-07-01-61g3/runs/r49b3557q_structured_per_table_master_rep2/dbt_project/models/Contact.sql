{{ config(materialized='table') }}

SELECT
    CAST(kontakt_id AS TEXT) AS "Id",
    INITCAP(TRIM(rufname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(familienname)), 'Unknown') AS "LastName",
    LOWER(TRIM(kontakt_email)) AS "Email",
    TRIM(tel) AS "Phone",
    INITCAP(TRIM(berufsbezeichnung)) AS "Title",
    CASE UPPER(TRIM(rolle))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'ESPANOL' THEN 'ES'
        WHEN 'SPANISH' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        WHEN 'ITALIANO' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CAST(kd_nummer AS TEXT) AS "AccountId",
    CAST(kontakt_id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_kontakte') }}