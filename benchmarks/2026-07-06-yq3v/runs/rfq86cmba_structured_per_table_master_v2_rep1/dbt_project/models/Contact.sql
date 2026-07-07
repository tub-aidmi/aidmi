-- models/Contact.sql
{{ config(materialized='table') }}

SELECT
    TRIM(k.kontakt_id) AS "Id",
    TRIM(INITCAP(k.rufname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(k.familienname)), 'Unknown') AS "LastName",
    TRIM(LOWER(k.kontakt_email)) AS "Email",
    TRIM(k.tel) AS "Phone",
    TRIM(k.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(k.rolle) IN ('entscheider', 'decision maker') THEN 'Decision Maker'
        WHEN LOWER(k.rolle) IN ('endbenutzer', 'end user') THEN 'End User'
        WHEN LOWER(k.rolle) IN ('technischer kontakt', 'technical contact') THEN 'Technical Contact'
        WHEN LOWER(k.rolle) IN ('executive sponsor', 'executive') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(k.korrespondenzsprache) = 'DE' THEN 'DE'
        WHEN UPPER(k.korrespondenzsprache) = 'EN' THEN 'EN'
        WHEN UPPER(k.korrespondenzsprache) = 'FR' THEN 'FR'
        WHEN UPPER(k.korrespondenzsprache) = 'ES' THEN 'ES'
        WHEN UPPER(k.korrespondenzsprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(k.kd_nummer) AS "AccountId",
    TRIM(k.kontakt_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS k