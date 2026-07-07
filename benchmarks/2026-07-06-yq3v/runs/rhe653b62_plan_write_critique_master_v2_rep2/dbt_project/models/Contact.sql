-- This dbt model transforms data from the master_kontakte source table
-- into the Contact target table schema.

{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id) AS "Id",
    TRIM(kontakte.rufname) AS "FirstName",
    COALESCE(TRIM(kontakte.familienname), 'Unknown') AS "LastName",
    LOWER(TRIM(kontakte.kontakt_email)) AS "Email",
    TRIM(kontakte.tel) AS "Phone",
    TRIM(kontakte.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) = 'end user' THEN 'End User'
        WHEN LOWER(kontakte.rolle) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(kontakte.korrespondenzsprache) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(kontakte.korrespondenzsprache)
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kontakte.kd_nummer) AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte