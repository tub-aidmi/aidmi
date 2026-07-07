-- {{ config(materialized='table') }}

SELECT
    TRIM(mk.kontakt_id) AS "Id",
    TRIM(mk.rufname) AS "FirstName",
    COALESCE(TRIM(mk.familienname), 'Unknown') AS "LastName",
    TRIM(LOWER(mk.kontakt_email)) AS "Email",
    TRIM(mk.tel) AS "Phone",
    TRIM(mk.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(mk.rolle)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(mk.rolle)) IN ('end user', 'endnutzer') THEN 'End User'
        WHEN LOWER(TRIM(mk.rolle)) IN ('technical contact', 'technischer kontakt') THEN 'Technical Contact'
        WHEN LOWER(TRIM(mk.rolle)) IN ('executive sponsor', 'executive') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) = 'de' THEN 'DE'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) = 'en' THEN 'EN'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) = 'fr' THEN 'FR'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) = 'es' THEN 'ES'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(mk.kd_nummer) AS "AccountId", -- This assumes AccountId maps directly to kd_nummer, which is the source customer ID.
    TRIM(mk.kontakt_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk