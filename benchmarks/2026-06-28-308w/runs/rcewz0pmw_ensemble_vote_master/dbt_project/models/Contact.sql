
{{ config(materialized='table') }}

SELECT
    kontakt.kontakt_id AS "Id",
    kontakt.rufname AS "FirstName",
    COALESCE(kontakt.familienname, '') AS "LastName",
    kontakt.kontakt_email AS "Email",
    kontakt.tel AS "Phone",
    kontakt.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(kontakt.rolle) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(kontakt.rolle) = 'end user' THEN 'End User'
        WHEN LOWER(kontakt.rolle) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(kontakt.rolle) = 'entscheider' THEN 'Decision Maker'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(kontakt.korrespondenzsprache) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(kontakt.korrespondenzsprache) IN ('de', 'deutsch') THEN 'DE'
        WHEN LOWER(kontakt.korrespondenzsprache) = 'fr' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    kontakt.kd_nummer AS "AccountId",
    kontakt.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kontakte') }} AS kontakt
