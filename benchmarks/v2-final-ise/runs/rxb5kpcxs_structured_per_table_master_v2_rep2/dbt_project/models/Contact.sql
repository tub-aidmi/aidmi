{{ config(materialized='table') }}

SELECT
     -- Id: Salesforce-style contact ID derived from kontakt_id using MD5
    '003' || SUBSTR(MD5(k.kontakt_id), 1, 15) AS "Id",

    -- FirstName from rufname
    CASE WHEN k.rufname IS NULL OR TRIM(k.rufname) = '' THEN NULL ELSE INITCAP(TRIM(k.rufname)) END AS "FirstName",

    -- LastName from familienname (NOT NULL target; default to safe value)
    COALESCE(NULLIF(TRIM(k.familienname), ''), 'Unknown') AS "LastName",

    -- Email
    CASE WHEN k.kontakt_email IS NULL OR TRIM(k.kontakt_email) = '' THEN NULL ELSE LOWER(TRIM(k.kontakt_email)) END AS "Email",

    -- Phone
    CASE WHEN k.tel IS NULL OR TRIM(k.tel) = '' THEN NULL ELSE TRIM(k.tel) END AS "Phone",

    -- Title from berufsbezeichnung
    CASE WHEN k.berufsbezeichnung IS NULL OR TRIM(k.berufsbezeichnung) = '' THEN NULL ELSE INITCAP(TRIM(k.berufsbezeichnung)) END AS "Title",

    -- Role__c: map source rolle values into the enum domain
    CASE UPPER(TRIM(k.rolle))
        WHEN 'ENTSCHEIDER'               THEN 'Decision Maker'
        WHEN 'END USER'                  THEN 'End User'
        WHEN 'TECHNISCHER KONTAKT'       THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR'         THEN 'Executive Sponsor'
        WHEN 'DECISION MAKER'            THEN 'Decision Maker'
        WHEN 'TECHNICAL CONTACT'         THEN 'Technical Contact'
        WHEN 'ENDUSER'                   THEN 'End User'
        WHEN 'ENTSCHEIDERIN'             THEN 'Decision Maker'
        ELSE NULL
    END AS "Role__c",

    -- Preferred_Language__c: normalize to DE/EN/FR/ES/IT
    CASE UPPER(TRIM(k.korrespondenzsprache))
        WHEN 'DE'   THEN 'DE'
        WHEN 'DEU'  THEN 'DE'
        WHEN 'GERMAN' THEN 'DE'
        WHEN 'EN'   THEN 'EN'
        WHEN 'ENG'  THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'FR'   THEN 'FR'
        WHEN 'FRE'  THEN 'FR'
        WHEN 'FRENCH' THEN 'FR'
        WHEN 'ES'   THEN 'ES'
        WHEN 'SPA'  THEN 'ES'
        WHEN 'SPANISH' THEN 'ES'
        WHEN 'IT'   THEN 'IT'
        WHEN 'ITA'  THEN 'IT'
        WHEN 'ITALIAN' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",

    -- AccountId: Salesforce-style Account Id derived from source customer number (kd_nummer)
    '001' || SUBSTR(MD5(k.kd_nummer), 1, 15) AS "AccountId",

    -- Legacy_Contact_ID__c: source natural key
    k.kontakt_id AS "Legacy_Contact_ID__c",

    -- CreatedDate / LastModifiedDate / IsDeleted: not present in source; NULL placeholder
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0          AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
WHERE k.kontakt_id IS NOT NULL
  AND TRIM(k.kontakt_id) != ''