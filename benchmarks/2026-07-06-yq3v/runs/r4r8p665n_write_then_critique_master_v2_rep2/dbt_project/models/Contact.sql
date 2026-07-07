-- depends_on: {{ ref('Account') }}
{{ config(materialized='table') }}

SELECT
    TRIM(k.kontakt_id) AS "Id",
    TRIM(k.rufname) AS "FirstName",
    COALESCE(TRIM(k.familienname), 'Unknown') AS "LastName",
    LOWER(TRIM(k.kontakt_email)) AS "Email",
    TRIM(k.tel) AS "Phone",
    TRIM(k.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(k.rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(k.rolle)) IN ('end user', 'endnutzer') THEN 'End User'
        WHEN LOWER(TRIM(k.rolle)) IN ('technical contact', 'technischer kontakt') THEN 'Technical Contact'
        WHEN LOWER(TRIM(k.rolle)) IN ('executive sponsor', 'führungskraft', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(k.kd_nummer) AS "AccountId", -- Assuming AccountId directly maps to kd_nummer for now; will be resolved by Account model's ID derivation.
    TRIM(k.kontakt_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS k