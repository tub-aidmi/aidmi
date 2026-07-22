{{ config(materialized='table') }}

SELECT
    '003' || LPAD(k.kontakt_id, 15, '0') AS "Id",
    TRIM(k.rufname) AS "FirstName",
    TRIM(k.familienname) AS "LastName",
    TRIM(k.kontakt_email) AS "Email",
    TRIM(k.tel) AS "Phone",
    TRIM(k.berufsbezeichnung) AS "Title",
    CASE
        WHEN UPPER(TRIM(k.rolle)) IN ('ENTSCHEIDER', 'DECISION MAKER') THEN 'Decision Maker'
        WHEN UPPER(TRIM(k.rolle)) IN ('ENDBENUTZER', 'END USER') THEN 'End User'
        WHEN UPPER(TRIM(k.rolle)) IN ('TECHNISCHER KONTAKT', 'TECHNICAL CONTACT') THEN 'Technical Contact'
        WHEN UPPER(TRIM(k.rolle)) IN ('GESCHÄFTSFÜHRUNG', 'EXECUTIVE SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ENGLISCH', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('FRANZÖSISCH', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('SPANISCH', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) IN ('ITALIENISCH', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE
        WHEN k.kd_nummer IS NOT NULL THEN '001' || LPAD(kd.kundennummer, 15, '0')
        ELSE NULL
    END AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} kd ON TRIM(k.kd_nummer) = TRIM(kd.kundennummer)