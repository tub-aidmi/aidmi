-- models/Contact.sql

{{ config(materialized='table') }}

SELECT
    TRIM(kontakt_id) AS "Id",
    TRIM(rufname) AS "FirstName",
    COALESCE(TRIM(familienname), 'Unknown') AS "LastName",
    TRIM(kontakt_email) AS "Email",
    TRIM(tel) AS "Phone",
    TRIM(berufsbezeichnung) AS "Title",
    CASE UPPER(TRIM(rolle))
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'ENDNUTZER' THEN 'End User'
        WHEN 'TECHNISCHER KONTAKT' THEN 'Technical Contact'
        WHEN 'FÜHRUNGSVERANTWORTLICHER' THEN 'Executive Sponsor'
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
    TRIM(kd_nummer) AS "AccountId",
    TRIM(kontakt_id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }}
