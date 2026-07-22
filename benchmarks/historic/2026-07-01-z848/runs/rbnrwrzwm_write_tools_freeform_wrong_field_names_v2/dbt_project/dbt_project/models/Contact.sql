{{ config(materialized='table') }}

SELECT
    ap_id AS "Id",
    vorname AS "FirstName",
    COALESCE(nachname, 'Unknown') AS "LastName", -- LastName is NOT NULL
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE
        WHEN TRIM(funktion) = 'Decision Maker' THEN 'Decision Maker'
        WHEN TRIM(funktion) = 'End User' THEN 'End User'
        WHEN TRIM(funktion) = 'Technical Contact' THEN 'Technical Contact'
        WHEN TRIM(funktion) = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(sprache) = 'DE' THEN 'DE'
        WHEN TRIM(sprache) = 'EN' THEN 'EN'
        WHEN TRIM(sprache) = 'FR' THEN 'FR'
        WHEN TRIM(sprache) = 'ES' THEN 'ES'
        WHEN TRIM(sprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunde AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
