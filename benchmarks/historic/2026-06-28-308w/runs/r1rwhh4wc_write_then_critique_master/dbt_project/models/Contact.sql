
{{ config(materialized='table') }}

SELECT
    kontakte.kontakt_id AS "Id",
    TRIM(INITCAP(kontakte.rufname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(kontakte.familienname)), 'Unknown') AS "LastName",
    kontakte.kontakt_email AS "Email",
    kontakte.tel AS "Phone",
    kontakte.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) = 'end user' THEN 'End User'
        WHEN LOWER(kontakte.rolle) IN ('entscheider', 'decision maker') THEN 'Decision Maker'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('de', 'deutsch') THEN 'DE'
        WHEN LOWER(kontakte.korrespondenzsprache) = 'fr' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    kontakte.kd_nummer AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kontakte') }} AS kontakte
