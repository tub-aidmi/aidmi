{{ config(materialized='table') }}

WITH contact_source AS (
    SELECT
        k.kontakt_id,
        k.rufname,
        k.familienname,
        LOWER(TRIM(k.kontakt_email)) AS kontakt_email,
        TRIM(k.tel) AS tel,
        INITCAP(TRIM(k.berufsbezeichnung)) AS berufsbezeichnung,
        UPPER(TRIM(k.rolle)) AS rolle,
        UPPER(TRIM(k.korrespondenzsprache)) AS korrespondenzsprache,
        TRIM(k.kd_nummer) AS kd_nummer_raw,
        c.unternehmensname
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c
        ON TRIM(k.kd_nummer) = TRIM(c.kundennummer)
)
SELECT
    CAST(UPPER(TRIM(kontakt_id)) AS TEXT) AS "Id",
    INITCAP(TRIM(rufname)) AS "FirstName",
    INITCAP(TRIM(familienname)) AS "LastName",
    kontakt_email AS "Email",
    tel AS "Phone",
    berufsbezeichnung AS "Title",
    CASE
        WHEN rolle IN ('DECISION MAKER', 'ENTSCHEIDER', 'DECISIONMAKER') THEN 'Decision Maker'
        WHEN rolle IN ('END USER', 'NUTZER', 'ENDUSER') THEN 'End User'
        WHEN rolle IN ('TECHNICAL CONTACT', 'TECHNISCHER KONTAKT', 'TECHCONTACT') THEN 'Technical Contact'
        WHEN rolle IN ('EXECUTIVE SPONSOR', 'GESCHAFTSFUEHRER', 'SPONSOR') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN korrespondenzsprache = 'DE' THEN 'DE'
        WHEN korrespondenzsprache = 'EN' THEN 'EN'
        WHEN korrespondenzsprache = 'FR' THEN 'FR'
        WHEN korrespondenzsprache = 'ES' THEN 'ES'
        WHEN korrespondenzsprache = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CAST(UPPER(TRIM(kd_nummer_raw)) AS TEXT) AS "AccountId",
    TRIM(kontakt_id) AS "Legacy_Contact_ID__c",
    '2024-01-01' AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_source
