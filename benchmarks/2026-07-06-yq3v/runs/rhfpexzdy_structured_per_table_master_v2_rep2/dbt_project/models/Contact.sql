-- dbt model for Contact

{{ config(materialized='table') }}

SELECT
    CAST(MD5(mk.kontakt_id) AS TEXT) AS "Id",
    TRIM(INITCAP(mk.rufname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(mk.familienname)), 'Unknown') AS "LastName",
    TRIM(LOWER(mk.kontakt_email)) AS "Email",
    TRIM(mk.tel) AS "Phone",
    TRIM(INITCAP(mk.berufsbezeichnung)) AS "Title",
    CASE
        WHEN LOWER(TRIM(mk.rolle)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(mk.rolle)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(mk.rolle)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(mk.rolle)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) IN ('de', 'german', 'deutsch') THEN 'DE'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) IN ('en', 'english') THEN 'EN'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) IN ('fr', 'french', 'französisch') THEN 'FR'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) IN ('es', 'spanish', 'spanisch') THEN 'ES'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) IN ('it', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CAST(MD5(mkun.kundennummer) AS TEXT) AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mkun
ON
    mk.kd_nummer = mkun.kundennummer