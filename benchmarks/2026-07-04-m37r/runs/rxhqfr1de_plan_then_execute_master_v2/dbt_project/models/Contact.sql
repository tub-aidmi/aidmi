{{ config(materialized='table') }}

SELECT
    MD5(kontakte.kontakt_id) AS "Id",
    INITCAP(TRIM(kontakte.rufname)) AS "FirstName",
    INITCAP(TRIM(COALESCE(kontakte.familienname, 'Unknown'))) AS "LastName",
    LOWER(TRIM(kontakte.kontakt_email)) AS "Email",
    TRIM(kontakte.tel) AS "Phone",
    INITCAP(TRIM(kontakte.berufsbezeichnung)) AS "Title",
    CASE UPPER(TRIM(kontakte.rolle))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(kontakte.korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kunden.kundennummer) AS "AccountId",
    kontakte.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
    ON kontakte.kd_nummer = kunden.kundennummer
