{{ config(materialized='table') }}

SELECT
    src_kontakt.kontakt_id AS "Id",
    TRIM(INITCAP(src_kontakt.rufname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(src_kontakt.familienname)), 'Unknown') AS "LastName",
    LOWER(TRIM(src_kontakt.kontakt_email)) AS "Email",
    TRIM(src_kontakt.tel) AS "Phone",
    TRIM(src_kontakt.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(src_kontakt.rolle)) IN ('technical contact', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(src_kontakt.rolle)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(src_kontakt.rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(src_kontakt.rolle)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(src_kontakt.korrespondenzsprache)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(src_kontakt.korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(src_kontakt.korrespondenzsprache)) IN ('fr', 'french', 'französisch') THEN 'FR'
        WHEN LOWER(TRIM(src_kontakt.korrespondenzsprache)) IN ('es') THEN 'ES'
        WHEN LOWER(TRIM(src_kontakt.korrespondenzsprache)) IN ('it') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    src_kunde.kundennummer AS "AccountId",
    src_kontakt.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS src_kontakt
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS src_kunde
    ON src_kontakt.kd_nummer = src_kunde.kundennummer
