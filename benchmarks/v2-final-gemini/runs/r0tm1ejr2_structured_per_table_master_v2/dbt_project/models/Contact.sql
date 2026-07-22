-- dbt model for Contact

{{ config(materialized='table') }}

SELECT
    MD5(k.kontakt_id) AS "Id",
    TRIM(k.rufname) AS "FirstName",
    COALESCE(TRIM(k.familienname), '') AS "LastName",
    TRIM(k.kontakt_email) AS "Email",
    TRIM(k.tel) AS "Phone",
    TRIM(k.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(k.rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(k.rolle)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(k.rolle)) IN ('techniker', 'technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(k.rolle)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('deutsch', 'de', 'german') THEN 'DE'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('englisch', 'en', 'english') THEN 'EN'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('französisch', 'fr', 'french') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE
        WHEN k.kd_nummer IS NOT NULL THEN MD5(k.kd_nummer)
        ELSE NULL
    END AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS k