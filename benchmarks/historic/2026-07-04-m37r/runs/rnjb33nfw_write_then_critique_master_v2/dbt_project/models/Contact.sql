{{ config(materialized='table') }}

SELECT
    mk.kontakt_id AS "Id",
    mk.rufname AS "FirstName",
    COALESCE(mk.familienname, 'Unknown') AS "LastName",
    mk.kontakt_email AS "Email",
    mk.tel AS "Phone",
    mk.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(TRIM(mk.rolle)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(mk.rolle)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(mk.rolle)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(mk.rolle)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(mk.korrespondenzsprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(mk.kd_nummer) AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
WHERE
    mk.kontakt_id IS NOT NULL
