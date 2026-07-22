{{ config(materialized='table') }}

SELECT
    -- Id: transform ap_id to Salesforce-style Account ID (same family as Customer IDs)
    '003' || CASE WHEN ap_id ~ '^\d+$' THEN LPAD(ap_id, 9, '0') ELSE RIGHT('000000000' || ap_id, 9) END AS "Id",

    -- FirstName
    INITCAP(TRIM(vorname)) AS "FirstName",

    -- LastName: NOT NULL - use fallback if missing
    COALESCE(INITCAP(TRIM(nachname)), 'Unknown') AS "LastName",

    -- Email
    LOWER(TRIM(email_adresse)) AS "Email",

    -- Phone
    TRIM(telefonnummer) AS "Phone",

    -- Title
    INITCAP(TRIM(position)) AS "Title",

    -- Role__c: map from funktion enum
    CASE UPPER(TRIM(funktion))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",

    -- Preferred_Language__c: map from sprache enum
    CASE UPPER(TRIM(sprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",

    -- AccountId: transform kunde to match Account.Id format (001 prefix)
    CASE WHEN TRIM(kunde) ~ '^\d+$'
        THEN '001' || LPAD(TRIM(kunde), 9, '0')
        ELSE NULL
    END AS "AccountId",

    -- Legacy_Contact_ID__c: natural key from source
    ap_id AS "Legacy_Contact_ID__c",

    -- CreatedDate
    CAST('2024-01-01' AS TEXT) AS "CreatedDate",

    -- LastModifiedDate
    CAST('2024-01-01' AS TEXT) AS "LastModifiedDate",

    -- IsDeleted
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
