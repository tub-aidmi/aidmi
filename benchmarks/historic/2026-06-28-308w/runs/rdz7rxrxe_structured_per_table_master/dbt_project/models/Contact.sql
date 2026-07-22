情緒{{ config(materialized='table') }}

SELECT
    kontakte.kontakt_id AS "Id",
    kontakte.rufname AS "FirstName",
    COALESCE(kontakte.familienname, '') AS "LastName",
    kontakte.kontakt_email AS "Email",
    kontakte.tel AS "Phone",
    kontakte.berufsbezeichnung AS "Title",
    CASE
        WHEN kontakte.rolle IN ('Decision Maker', 'Entscheider') THEN 'Decision Maker'
        WHEN kontakte.rolle = 'End User' THEN 'End User'
        WHEN kontakte.rolle = 'Technical Contact' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN kontakte.korrespondenzsprache IN ('DE', 'Deutsch', 'de') THEN 'DE'
        WHEN kontakte.korrespondenzsprache IN ('EN', 'Englisch', 'English') THEN 'EN'
        WHEN kontakte.korrespondenzsprache = 'FR' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    kontakte.kd_nummer AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kontakte') }} AS kontakte