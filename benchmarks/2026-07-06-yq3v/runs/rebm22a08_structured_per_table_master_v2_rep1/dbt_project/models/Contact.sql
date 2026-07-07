-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    TRIM(mk.kontakt_id) AS "Id",
    INITCAP(TRIM(mk.rufname)) AS "FirstName",
    INITCAP(TRIM(mk.familienname)) AS "LastName",
    LOWER(TRIM(mk.kontakt_email)) AS "Email",
    TRIM(mk.tel) AS "Phone",
    INITCAP(TRIM(mk.berufsbezeichnung)) AS "Title",
    CASE
        WHEN LOWER(TRIM(mk.rolle)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(mk.rolle)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(mk.rolle)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(mk.rolle)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(mk.kd_nummer) AS "AccountId",
    TRIM(mk.kontakt_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk