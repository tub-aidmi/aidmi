-- models/Contact.sql
{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id) AS "Id",
    kontakte.rufname AS "FirstName",
    COALESCE(kontakte.familienname, kontakte.kontakt_id) AS "LastName",
    kontakte.kontakt_email AS "Email",
    kontakte.tel AS "Phone",
    kontakte.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) = 'end user' THEN 'End User'
        WHEN LOWER(kontakte.rolle) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(kontakte.korrespondenzsprache) = 'DE' THEN 'DE'
        WHEN UPPER(kontakte.korrespondenzsprache) = 'EN' THEN 'EN'
        WHEN UPPER(kontakte.korrespondenzsprache) = 'FR' THEN 'FR'
        WHEN UPPER(kontakte.korrespondenzsprache) = 'ES' THEN 'ES'
        WHEN UPPER(kontakte.korrespondenzsprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kontakte.kd_nummer) AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NOW()::text AS "CreatedDate",
    NOW()::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
