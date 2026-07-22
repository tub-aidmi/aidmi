-- depends_on: {{ ref('Account') }} removed as per instructions
{{ config(materialized='table') }}

SELECT
    kontakte.kontakt_id AS "Id",
    kontakte.rufname AS "FirstName",
    COALESCE(kontakte.familienname, '') AS "LastName",
    kontakte.kontakt_email AS "Email",
    kontakte.tel AS "Phone",
    kontakte.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) = 'end user' THEN 'End User'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('de', 'deutsch') THEN 'DE'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(kontakte.korrespondenzsprache) = 'fr' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunden.kundennummer AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_kontakte') }} AS kontakte
LEFT JOIN
    {{ source('fixture_master_src', 'master_kunden') }} AS kunden
ON
    kontakte.kd_nummer = kunden.kundennummer