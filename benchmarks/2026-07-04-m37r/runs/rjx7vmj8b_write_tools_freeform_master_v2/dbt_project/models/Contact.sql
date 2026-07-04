-- models/Contact.sql
{{ config(materialized='table') }}

SELECT
    MD5(kontakt_id) AS "Id",
    TRIM(rufname) AS "FirstName",
    COALESCE(TRIM(familienname), 'Unknown') AS "LastName", -- LastName is NOT NULL
    TRIM(kontakt_email) AS "Email",
    TRIM(tel) AS "Phone",
    TRIM(berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(rolle) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(rolle) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(rolle) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(rolle) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(korrespondenzsprache) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(korrespondenzsprache) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(korrespondenzsprache) IN ('fr', 'french', 'französisch') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kd_nummer) AS "AccountId", -- Use consistent Account Id generation
    TRIM(kontakt_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }}
