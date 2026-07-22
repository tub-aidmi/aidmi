
-- dbt model for Contact

{{ config(materialized='table') }}

SELECT
    TRIM(kontakte.kontakt_id) AS "Id",
    TRIM(kontakte.rufname) AS "FirstName",
    COALESCE(NULLIF(TRIM(kontakte.familienname), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(kontakte.kontakt_email)) AS "Email",
    TRIM(kontakte.tel) AS "Phone",
    TRIM(kontakte.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(kontakte.rolle)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(kontakte.rolle)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(kontakte.rolle)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(kontakte.rolle)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(kontakte.korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    NULLIF(TRIM(kontakte.kd_nummer), '') AS "AccountId",
    NULLIF(TRIM(kontakte.kontakt_id), '') AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kontakte') }} AS kontakte
