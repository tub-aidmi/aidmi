{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id) AS "Id",
    kontakte.rufname AS "FirstName",
    kontakte.familienname AS "LastName",
    kontakte.kontakt_email AS "Email",
    kontakte.tel AS "Phone",
    kontakte.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) = 'entscheider' THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) = 'endbenutzer' THEN 'End User'
        WHEN LOWER(kontakte.rolle) = 'technischer kontakt' THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) = 'führungskraft' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(kontakte.korrespondenzsprache) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(kontakte.korrespondenzsprache) IN ('EN', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(kontakte.korrespondenzsprache) IN ('FR', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(kontakte.korrespondenzsprache) IN ('ES', 'SPANISCH') THEN 'ES'
        WHEN UPPER(kontakte.korrespondenzsprache) IN ('IT', 'ITALIENISCH') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kunden.kundennummer) AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
    ON kontakte.kd_nummer = kunden.kundennummer
