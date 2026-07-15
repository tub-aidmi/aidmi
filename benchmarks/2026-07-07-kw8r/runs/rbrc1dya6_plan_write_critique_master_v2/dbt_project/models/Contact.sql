{{ config(materialized='table') }}

SELECT
    'C0XX' || COALESCE(TRIM(k.kontakt_id), '') AS "Id",
    COALESCE(NULLIF(TRIM(k.rufname), ''), 'Unknown') AS "FirstName",
    COALESCE(NULLIF(TRIM(k.familienname), ''), 'Unspecified') AS "LastName",
    LOWER(TRIM(k.kontakt_email)) AS "Email",
    TRIM(k.tel) AS "Phone",
    INITCAP(TRIM(k.berufsbezeichnung)) AS "Title",
    CASE LOWER(TRIM(k.rolle))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(k.korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    'A0XX' || COALESCE(TRIM(a.kundennummer), '') AS "AccountId",
    TRIM(k.kontakt_id) AS "Legacy_Contact_ID__c",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} a
    ON TRIM(k.kd_nummer) = TRIM(a.kundennummer)