-- models/Contact.sql
{{ config(materialized='table') }}

SELECT
    MD5(TRIM(t1.kontakt_id)) AS "Id",
    INITCAP(TRIM(t1.rufname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(t1.familienname)), 'Unknown') AS "LastName",
    TRIM(LOWER(t1.kontakt_email)) AS "Email",
    TRIM(t1.tel) AS "Phone",
    INITCAP(TRIM(t1.berufsbezeichnung)) AS "Title",
    CASE
        WHEN LOWER(TRIM(t1.rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(t1.rolle)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(t1.rolle)) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(t1.rolle)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(t1.korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(t1.korrespondenzsprache)) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(TRIM(t1.korrespondenzsprache)) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(TRIM(t1.korrespondenzsprache)) IN ('es', 'spanisch', 'spanish') THEN 'ES'
        WHEN LOWER(TRIM(t1.korrespondenzsprache)) IN ('it', 'italienisch', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(TRIM(t1.kd_nummer)) AS "AccountId",
    t1.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS t1