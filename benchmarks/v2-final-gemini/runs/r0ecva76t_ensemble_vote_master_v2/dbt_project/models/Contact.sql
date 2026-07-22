{{ config(materialized='table') }}

WITH
contact_source AS (
    SELECT
        kontakt_id,
        rufname,
        familienname,
        kontakt_email,
        tel,
        berufsbezeichnung,
        rolle,
        korrespondenzsprache,
        kd_nummer
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
),
kunden_source AS (
    SELECT
        kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)

SELECT
    TRIM(cs.kontakt_id) AS "Id",
    TRIM(cs.rufname) AS "FirstName",
    COALESCE(TRIM(cs.familienname), 'Unknown') AS "LastName",
    TRIM(cs.kontakt_email) AS "Email",
    TRIM(cs.tel) AS "Phone",
    TRIM(cs.berufsbezeichnung) AS "Title",
    CASE TRIM(LOWER(cs.rolle))
        WHEN 'entscheider' THEN 'Decision Maker'
        WHEN 'endnutzer' THEN 'End User'
        WHEN 'technischer ansprechpartner' THEN 'Technical Contact'
        WHEN 'führungskraft' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE TRIM(LOWER(cs.korrespondenzsprache))
        WHEN 'deutsch' THEN 'DE'
        WHEN 'englisch' THEN 'EN'
        WHEN 'französisch' THEN 'FR'
        WHEN 'spanisch' THEN 'ES'
        WHEN 'italienisch' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(ks.kundennummer) AS "AccountId",
    TRIM(cs.kontakt_id) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM contact_source cs
LEFT JOIN kunden_source ks ON TRIM(cs.kd_nummer) = TRIM(ks.kundennummer)
