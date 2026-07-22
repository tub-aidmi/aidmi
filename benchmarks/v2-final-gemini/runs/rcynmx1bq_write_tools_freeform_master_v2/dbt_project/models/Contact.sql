{{ config(materialized='table') }}

SELECT
    MD5(TRIM(kontakt_id)) AS "Id",
    TRIM(rufname) AS "FirstName",
    COALESCE(TRIM(familienname), 'Unknown') AS "LastName",
    TRIM(kontakt_email) AS "Email",
    TRIM(tel) AS "Phone",
    TRIM(berufsbezeichnung) AS "Title",
    CASE LOWER(TRIM(rolle))
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'technischer ansprechpartner' THEN 'Technical Contact'
        WHEN 'end user' THEN 'End User'
        WHEN 'endanwender' THEN 'End User'
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'entscheider' THEN 'Decision Maker'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE LOWER(TRIM(korrespondenzsprache))
        WHEN 'en' THEN 'EN'
        WHEN 'english' THEN 'EN'
        WHEN 'englisch' THEN 'EN'
        WHEN 'de' THEN 'DE'
        WHEN 'deutsch' THEN 'DE'
        WHEN 'german' THEN 'DE'
        WHEN 'fr' THEN 'FR'
        WHEN 'french' THEN 'FR'
        WHEN 'es' THEN 'ES'
        WHEN 'spanish' THEN 'ES'
        WHEN 'it' THEN 'IT'
        WHEN 'italian' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(TRIM(kd_nummer)) AS "AccountId",
    TRIM(kontakt_id) AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}