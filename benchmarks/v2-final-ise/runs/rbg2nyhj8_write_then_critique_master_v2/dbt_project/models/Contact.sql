{{ config(materialized='table') }}

WITH src_contact AS (
    SELECT *
    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
),
src_customer AS (
    SELECT *
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
)
SELECT
    -- Deterministic Salesforce-style Contact Id: '003' prefix + 15-char hash of primary key
    CONCAT('003', SUBSTRING(MD5(k.kontakt_id::TEXT), 1, 15)) AS "Id",

    -- First name (from rufname)
    TRIM(k.rufname) AS "FirstName",

    -- Last name (from familienname) — meaningful default instead of empty string for NOT NULL
    COALESCE(TRIM(k.familienname), 'Unknown') AS "LastName",

    -- Email
    TRIM(k.kontakt_email) AS "Email",

    -- Phone
    TRIM(k.tel) AS "Phone",

    -- Job title
    TRIM(k.berufsbezeichnung) AS "Title",

    -- Role mapping to enum: Decision Maker, End User, Technical Contact, Executive Sponsor
    CASE UPPER(TRIM(k.rolle))
        WHEN 'DECISION MAKER'  THEN 'Decision Maker'
        WHEN 'END USER'        THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN 'ENTSCHEIDUNGSTRÄGER' THEN 'Decision Maker'
        WHEN 'ENDANWENDER'     THEN 'End User'
        WHEN 'TECHNISCHER KONTAKT' THEN 'Technical Contact'
        WHEN 'FUEHRUNGSUNTERSTÜTZUNG' THEN 'Executive Sponsor'
        WHEN 'FUHRENUNTERSTÜTZUNG' THEN 'Executive Sponsor'
        WHEN 'FUEHRUNGSKRAFT'  THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",

    -- Preferred Language mapping — normalize to ISO codes, uppercase
    CASE UPPER(TRIM(k.korrespondenzsprache))
        WHEN 'DE'        THEN 'DE'
        WHEN 'EN'        THEN 'EN'
        WHEN 'FR'        THEN 'FR'
        WHEN 'ES'        THEN 'ES'
        WHEN 'IT'        THEN 'IT'
        WHEN 'DEUTSCH'   THEN 'DE'
        WHEN 'ENGLISH'   THEN 'EN'
        WHEN 'FRENCH'    THEN 'FR'
        WHEN 'SPANISH'   THEN 'ES'
        WHEN 'ITALIAN'   THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",

    -- AccountId: reference the Salesforce-style Account Id — use the same deterministic key transform as the Account model
    CONCAT('001', SUBSTRING(MD5(c.kundennummer::TEXT), 1, 15)) AS "AccountId",

    -- Legacy Contact ID from source primary key
    k.kontakt_id AS "Legacy_Contact_ID__c",

    -- System fields (no source data available — NULL for dates, 0 for IsDeleted)
    NULL :: TEXT AS "CreatedDate",
    NULL :: TEXT AS "LastModifiedDate",
    0 :: INTEGER AS "IsDeleted"

FROM src_contact k
INNER JOIN src_customer c
    ON TRIM(k.kd_nummer) = TRIM(c.kundennummer)

WHERE k.kontakt_id IS NOT NULL;