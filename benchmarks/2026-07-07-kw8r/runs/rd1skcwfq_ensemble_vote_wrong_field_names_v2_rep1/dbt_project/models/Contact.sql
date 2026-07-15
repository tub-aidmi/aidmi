{{ config(materialized='table') }}

SELECT
    CAST(ap_id AS TEXT) AS "Id",
    INITCAP(TRIM(vorname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(nachname)), 'Unknown') AS "LastName",
    TRIM(email_adresse) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    INITCAP(TRIM(position)) AS "Title",
    CASE
        WHEN UPPER(TRIM(funktion)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(funktion)) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(funktion)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(TRIM(funktion)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(sprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(sprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(kunde) AS "AccountId",
    CAST(ap_id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}