{{ config(materialized='table') }}

SELECT
    k.kontakt_id AS "Id",
    k.rufname AS "FirstName",
    COALESCE(k.familienname, 'N/A') AS "LastName",
    k.kontakt_email AS "Email",
    k.tel AS "Phone",
    k.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(TRIM(k.rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(k.rolle)) IN ('end user', 'endnutzer') THEN 'End User'
        WHEN LOWER(TRIM(k.rolle)) IN ('technical contact', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(k.rolle)) IN ('executive sponsor', 'sponsor', 'führungskraft') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('es', 'spanisch', 'spanish') THEN 'ES'
        WHEN LOWER(TRIM(k.korrespondenzsprache)) IN ('it', 'italienisch', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    cust.kundennummer AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS k
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS cust
ON
    k.kd_nummer = cust.kundennummer
