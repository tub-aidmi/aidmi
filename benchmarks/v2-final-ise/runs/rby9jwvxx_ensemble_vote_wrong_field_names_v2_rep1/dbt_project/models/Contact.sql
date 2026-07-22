{{ config(materialized='table') }}

-- Model: Contact
-- Source: fixture_wrong_field_names_v2_src.ansprechpartner (contacts / Ansprechpartner in German)
-- Transforms German source columns into Salesforce-style Contact schema.

SELECT
    -- Primary identity and legacy reference
    CAST(TRIM(ap.ap_id) AS TEXT) AS "Id",
    CAST(TRIM(ap.ap_id) AS TEXT) AS "Legacy_Contact_ID__c",

    -- Name fields (German: vorname = first name, nachname = last name)
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    INITCAP(TRIM(ap.nachname)) AS "LastName",

    -- Contact details
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",

    -- Role mapping: translate German function values to English role enum
    CASE UPPER(REGEXP_REPLACE(TRIM(ap.funktion), '[^A-ZÄÖÜÄ]', '', 'g'))
        WHEN 'ENTSCHEIDER'              THEN 'Decision Maker'
        WHEN 'ENTSCHEIDUNGSLEITER'      THEN 'Decision Maker'
        WHEN 'KEYACCOUNT'               THEN 'Decision Maker'
        WHEN 'ENDUSER'                  THEN 'End User'
        WHEN 'ENDANWENDER'              THEN 'End User'
        WHEN 'NUTZER'                   THEN 'End User'
        WHEN 'TECHNISCH'                THEN 'Technical Contact'
        WHEN 'TECHNIK'                  THEN 'Technical Contact'
        WHEN 'SUPPORT'                  THEN 'Technical Contact'
        WHEN 'ADMINISTRATOR'            THEN 'Technical Contact'
        WHEN 'GESCHAEFTSFUEHRER'        THEN 'Executive Sponsor'
        WHEN 'VORSTAND'                 THEN 'Executive Sponsor'
        WHEN 'GEOFUERHER'               THEN 'Executive Sponsor'
        WHEN 'GELEITER'                 THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",

    -- Preferred language mapping: German source to ISO 2-letter codes
    CASE UPPER(TRIM(ap.sprache))
        WHEN 'DE'            THEN 'DE'
        WHEN 'GERMAN'        THEN 'DE'
        WHEN 'DEUTSCH'       THEN 'DE'
        WHEN 'EN'            THEN 'EN'
        WHEN 'ENGLISH'       THEN 'EN'
        WHEN 'FR'            THEN 'FR'
        WHEN 'FRENCH'        THEN 'FR'
        WHEN 'FRANCOIS'      THEN 'FR'
        WHEN 'ES'            THEN 'ES'
        WHEN 'SPANISH'       THEN 'ES'
        WHEN 'ESPANOL'       THEN 'ES'
        WHEN 'IT'            THEN 'IT'
        WHEN 'ITALIAN'       THEN 'IT'
        WHEN 'ITAELNIISC'    THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",

    -- AccountId: resolve customer reference from source to canonical customer number
    -- LEFT JOIN to Kunden table ensures we use the same customer key transformation
    -- as applied in the Account model (customers.kunden_nr).
    k.kunden_nr AS "AccountId",

    -- Audit fields — not present in source; set defaults for initial load
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap

-- Join to Kunden to canonicalize the customer reference (AccountId)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(UPPER(ap.kunde)) = TRIM(UPPER(k.kunden_nr))