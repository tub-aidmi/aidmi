
{{ config(materialized='table') }}

SELECT
    kontakt_id AS "Id",
    rufname AS "FirstName",
    familienname AS "LastName",
    kontakt_email AS "Email",
    tel AS "Phone",
    berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(rolle) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(rolle) = 'entscheider' THEN 'Decision Maker'
        WHEN LOWER(rolle) = 'end user' THEN 'End User'
        WHEN LOWER(rolle) = 'technical contact' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(korrespondenzsprache) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(korrespondenzsprache) IN ('de', 'deutsch') THEN 'DE'
        WHEN LOWER(korrespondenzsprache) = 'fr' THEN 'FR'
        WHEN LOWER(korrespondenzsprache) = 'es' THEN 'ES'
        WHEN LOWER(korrespondenzsprache) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kd_nummer AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kontakte') }}
