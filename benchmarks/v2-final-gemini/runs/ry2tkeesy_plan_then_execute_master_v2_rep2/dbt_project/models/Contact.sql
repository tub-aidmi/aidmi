-- depends_on: {{ source('fixture_master_v2_src', 'master_kontakte') }}
{{ config(materialized='table') }}

SELECT
    TRIM(kontakt_id) AS "Id",
    TRIM(rufname) AS "FirstName",
    COALESCE(TRIM(familienname), 'Unknown') AS "LastName",
    LOWER(TRIM(kontakt_email)) AS "Email",
    TRIM(tel) AS "Phone",
    TRIM(berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(rolle)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(rolle)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(rolle)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(rolle)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(korrespondenzsprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(korrespondenzsprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(korrespondenzsprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(korrespondenzsprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(korrespondenzsprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(kd_nummer) AS "AccountId",
    TRIM(kontakt_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }}