{{ config(materialized='table') }}

SELECT
    MD5(ap_id) AS "Id",
    vorname AS "FirstName",
    COALESCE(nachname, 'Unknown') AS "LastName", -- LastName is NOT NULL
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE
        WHEN LOWER(funktion) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(funktion) = 'end user' THEN 'End User'
        WHEN LOWER(funktion) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(funktion) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(sprache) = 'DE' THEN 'DE'
        WHEN UPPER(sprache) = 'EN' THEN 'EN'
        WHEN UPPER(sprache) = 'FR' THEN 'FR'
        WHEN UPPER(sprache) = 'ES' THEN 'ES'
        WHEN UPPER(sprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kunde) AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP AS "CreatedDate",
    CURRENT_TIMESTAMP AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
