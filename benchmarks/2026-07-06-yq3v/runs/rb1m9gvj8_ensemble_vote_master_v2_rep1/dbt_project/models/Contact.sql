{{ config(materialized='table') }}

WITH source_contacts AS (
    SELECT
        kontakt_id,
        rufname,
        familienname,
        kontakt_email,
        tel,
        berufsbezeichnung,
        rolle,
        korrespondenzsprache,
        kd_nummer
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
),
source_customers AS (
    SELECT
        kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
    TRIM(sc.kontakt_id) AS "Id",
    TRIM(sc.rufname) AS "FirstName",
    COALESCE(TRIM(sc.familienname), 'Unknown') AS "LastName",
    TRIM(sc.kontakt_email) AS "Email",
    TRIM(sc.tel) AS "Phone",
    TRIM(sc.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(sc.rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(sc.rolle)) IN ('end user', 'endnutzer') THEN 'End User'
        WHEN LOWER(TRIM(sc.rolle)) IN ('technical contact', 'technischer kontakt') THEN 'Technical Contact'
        WHEN LOWER(TRIM(sc.rolle)) IN ('executive sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(sc.korrespondenzsprache)) IN ('de', 'deutsch') THEN 'DE'
        WHEN LOWER(TRIM(sc.korrespondenzsprache)) IN ('en', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(sc.korrespondenzsprache)) IN ('fr', 'französisch') THEN 'FR'
        WHEN LOWER(TRIM(sc.korrespondenzsprache)) IN ('es', 'spanisch') THEN 'ES'
        WHEN LOWER(TRIM(sc.korrespondenzsprache)) IN ('it', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(sc.kd_nummer) AS "AccountId", -- AccountId is the customer number for the linked Account
    TRIM(sc.kontakt_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_contacts sc