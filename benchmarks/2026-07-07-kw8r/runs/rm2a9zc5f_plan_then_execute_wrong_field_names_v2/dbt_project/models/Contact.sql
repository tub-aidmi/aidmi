{{ config(materialized='table') }}

SELECT
    '003' || ap_id AS "Id",
    TRIM(INITCAP(vorname)) AS "FirstName",
    COALESCE(NULLIF(TRIM(INITCAP(nachname)), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(email_adresse)) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    TRIM(INITCAP(position)) AS "Title",
    CASE UPPER(TRIM(funktion))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(sprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || kunde AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}