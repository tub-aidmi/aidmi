{{ config(materialized='table') }}

SELECT
    kontakte.kontakt_id AS "Id",
    kontakte.rufname AS "FirstName",
    COALESCE(kontakte.familienname, 'Unknown') AS "LastName",
    kontakte.kontakt_email AS "Email",
    kontakte.tel AS "Phone",
    kontakte.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) LIKE '%entscheider%' THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) LIKE '%techniker%' THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) LIKE '%endnutzer%' THEN 'End User'
        WHEN LOWER(kontakte.rolle) LIKE '%geschäftsführer%' THEN 'Executive Sponsor'
        WHEN LOWER(kontakte.rolle) LIKE '%manager%' THEN 'Decision Maker' -- Added common English term
        WHEN LOWER(kontakte.rolle) LIKE '%supervisor%' THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) LIKE '%admin%' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(kontakte.korrespondenzsprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT')
            THEN UPPER(TRIM(kontakte.korrespondenzsprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    kontakte.kd_nummer AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate", -- Not available in source
    NULL AS "LastModifiedDate", -- Not available in source
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
