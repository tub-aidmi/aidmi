{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id) AS "Id",
    kontakte.rufname AS "FirstName",
    kontakte.familienname AS "LastName",
    kontakte.kontakt_email AS "Email",
    kontakte.tel AS "Phone",
    kontakte.berufsbezeichnung AS "Title",
    CASE
        WHEN LOWER(kontakte.rolle) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(kontakte.rolle) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(kontakte.rolle) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(kontakte.rolle) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('deutsch', 'de', 'german') THEN 'DE'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('englisch', 'en', 'english') THEN 'EN'
        WHEN LOWER(kontakte.korrespondenzsprache) IN ('französisch', 'fr', 'french') THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kunden.kundennummer) AS "AccountId", -- Joining to get the Account Id
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
ON
    kontakte.kd_nummer = kunden.kundennummer
