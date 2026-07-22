{{ config(materialized='table') }}

SELECT
    -- Generate deterministic Salesforce-style Contact Id from ap_id
    '003' || SUBSTR(MD5(ap.ap_id), 1, 12) AS "Id",

    -- FirstName from vorname, trimmed
    TRIM(ap.vorname) AS "FirstName",

    -- LastName NOT NULL: default to 'Unknown' when missing
    COALESCE(TRIM(ap.nachname), 'Unknown') AS "LastName",

    -- Email lowercased and trimmed
    LOWER(TRIM(ap.email_adresse)) AS "Email",

    -- Phone trimmed
    TRIM(ap.telefonnummer) AS "Phone",

    -- Title from position, capitalised
    INITCAP(TRIM(ap.position)) AS "Title",

    -- Map funktion to Role__c enum
    CASE
        WHEN UPPER(TRIM(ap.funktion)) IN ('CEO', 'CFOR', 'MD', 'MANAGING DIRECTOR')
          OR UPPER(TRIM(ap.funktion)) LIKE '%LEITER%'
          OR UPPER(TRIM(ap.funktion)) LIKE '%GF%'
          OR UPPER(TRIM(ap.funktion)) LIKE '%GESCHÄFTSFÜHRER%'
          OR UPPER(TRIM(ap.funktion)) LIKE '%DIREKTOR%'
          OR UPPER(TRIM(ap.funktion)) LIKE '%VORSTAND%'
          OR UPPER(TRIM(ap.funktion)) IN ('EINKÄUFER', 'BUYER') THEN 'Decision Maker'

        WHEN UPPER(TRIM(ap.funktion)) LIKE '%TECHNIK%'
          OR UPPER(TRIM(ap.funktion)) LIKE '%ENGINEER%'
          OR UPPER(TRIM(ap.funktion)) LIKE '%INGENIEUR%'
          OR UPPER(TRIM(ap.funktion)) LIKE '%IT%'
          OR UPPER(TRIM(ap.funktion)) LIKE '%TECHNISCH%'
          OR UPPER(TRIM(ap.funktion)) LIKE '%SUPPORT%' THEN 'Technical Contact'

        WHEN UPPER(TRIM(ap.funktion)) LIKE '%SPONSOR%' THEN 'Executive Sponsor'

        ELSE 'End User'
    END AS "Role__c",

    -- Map sprache to Preferred_Language__c enum (DE, EN, FR, ES, IT)
    CASE
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'DEU', 'DEUTSCH', 'D')
          OR UPPER(TRIM(ap.sprache)) LIKE '%GERMAN%' THEN 'DE'
        WHEN UPPER(TRIM(ap.sprache)) IN ('EN', 'ENG', 'ENGLISCH', 'E', 'GBR', 'USA')
          OR UPPER(TRIM(ap.sprache)) LIKE '%ENGLISH%' THEN 'EN'
        WHEN UPPER(TRIM(ap.sprache)) IN ('FR', 'FRE', 'FRA', 'FRANZÖSISCH', 'F')
          OR UPPER(TRIM(ap.sprache)) LIKE '%FRENCH%' THEN 'FR'
        WHEN UPPER(TRIM(ap.sprache)) IN ('ES', 'ESP', 'SPANISCH', 'E')
          OR UPPER(TRIM(ap.sprache)) LIKE '%SPANISH%' THEN 'ES'
        WHEN UPPER(TRIM(ap.sprache)) IN ('IT', 'ITA', 'ITALIENISCH', 'I')
          OR UPPER(TRIM(ap.sprache)) LIKE '%ITALIAN%' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",

    -- AccountId: derive Salesforce-style Account Id (001-*) by joining to kunden table
    CASE WHEN ap.kunde IS NOT NULL THEN
        '001' || SUBSTR(MD5(TRIM(k.kunden_nr)), 1, 12)
    ELSE
        NULL
    END AS "AccountId",

    -- Legacy_Contact_ID__c from source natural key ap_id
    ap.ap_id AS "Legacy_Contact_ID__c",

    -- Standard audit fields (initial load: current date; IsDeleted = false)
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(ap.kunde) = TRIM(k.kunden_nr)

WHERE ap.nachname IS NOT NULL
   OR ap.vorname IS NOT NULL;