{{ config(materialized='table') }}

SELECT
    MD5(TRIM(kontakt_id)) AS "Id",
    TRIM(rufname) AS "FirstName",
    COALESCE(TRIM(familienname), 'Unknown') AS "LastName",
    TRIM(kontakt_email) AS "Email",
    TRIM(tel) AS "Phone",
    TRIM(berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(rolle)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(rolle)) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(rolle)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(TRIM(korrespondenzsprache)) IN ('fr', 'französisch', 'french') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(TRIM(kd_nummer)) AS "AccountId",
    TRIM(kontakt_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }}
