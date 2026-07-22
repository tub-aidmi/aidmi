{{ config(materialized='table') }}

SELECT
    kontakte.kontakt_id AS "Id",
    TRIM(kontakte.rufname) AS "FirstName",
    COALESCE(TRIM(kontakte.familienname), '') AS "LastName",
    TRIM(kontakte.kontakt_email) AS "Email",
    TRIM(kontakte.tel) AS "Phone",
    TRIM(kontakte.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) = 'entscheider' THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) = 'endnutzer' THEN 'End User'
        WHEN LOWER(kontakte.rolle) = 'technischer kontakt' THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) = 'sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(kontakte.korrespondenzsprache) = 'deutsch' OR LOWER(kontakte.korrespondenzsprache) = 'de' THEN 'DE'
        WHEN LOWER(kontakte.korrespondenzsprache) = 'englisch' OR LOWER(kontakte.korrespondenzsprache) = 'en' THEN 'EN'
        WHEN LOWER(kontakte.korrespondenzsprache) = 'französisch' OR LOWER(kontakte.korrespondenzsprache) = 'fr' THEN 'FR'
        WHEN LOWER(kontakte.korrespondenzsprache) = 'spanisch' OR LOWER(kontakte.korrespondenzsprache) = 'es' THEN 'ES'
        WHEN LOWER(kontakte.korrespondenzsprache) = 'italienisch' OR LOWER(kontakte.korrespondenzsprache) = 'it' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kunden.kundennummer) AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
ON
    kontakte.kd_nummer = kunden.kundennummer
