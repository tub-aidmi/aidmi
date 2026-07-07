{{ config(materialized='table') }}

SELECT
    MD5(k.kontakt_id) AS "Id",
    TRIM(k.rufname) AS "FirstName",
    COALESCE(TRIM(k.familienname), 'Unknown') AS "LastName",
    LOWER(TRIM(k.kontakt_email)) AS "Email",
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
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('fr', 'französisch', 'french') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(ku.kundennummer) AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS k
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS ku
ON
    k.kd_nummer = ku.kundennummer
