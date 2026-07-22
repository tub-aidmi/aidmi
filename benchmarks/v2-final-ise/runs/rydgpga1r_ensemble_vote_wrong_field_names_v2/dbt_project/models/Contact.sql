{{ config(materialized='table') }}

SELECT
    CAST(ap_id AS TEXT) AS "Id",
    CAST(vorname AS TEXT) AS "FirstName",
    CAST(nachname AS TEXT) AS "LastName",
    CAST(email_adresse AS TEXT) AS "Email",
    CAST(telefonnummer AS TEXT) AS "Phone",
    CAST(position AS TEXT) AS "Title",
    CASE LOWER(TRIM(funktion))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
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
    CAST(kunde AS TEXT) AS "AccountId",
    CAST(ap_id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}