{{ config(materialized='table') }}

SELECT
    k.kontakt_id AS "Id",
    k.rufname AS "FirstName",
    COALESCE(k.familienname, 'Unknown') AS "LastName",
    k.kontakt_email AS "Email",
    k.tel AS "Phone",
    k.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(TRIM(k.rolle)) IN ('decision maker', 'entscheider', 'decisionmaker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(k.rolle)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(k.rolle)) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(k.rolle)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('fr', 'französisch', 'french') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    k.kd_nummer AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS k
