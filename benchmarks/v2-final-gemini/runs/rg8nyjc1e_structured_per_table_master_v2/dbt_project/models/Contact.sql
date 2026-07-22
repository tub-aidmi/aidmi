-- noinspection SqlNoDataSourceInspectionForFile

{{ config(materialized='table') }}

SELECT
    kontakt.kontakt_id AS "Id",
    kontakt.rufname AS "FirstName",
    kontakt.familienname AS "LastName",
    kontakt.kontakt_email AS "Email",
    kontakt.tel AS "Phone",
    kontakt.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(TRIM(kontakt.rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(kontakt.rolle)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(kontakt.rolle)) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(kontakt.rolle)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(kontakt.korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(kontakt.korrespondenzsprache)) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(TRIM(kontakt.korrespondenzsprache)) IN ('fr', 'französisch', 'french') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    kontakt.kd_nummer AS "AccountId",
    kontakt.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakt