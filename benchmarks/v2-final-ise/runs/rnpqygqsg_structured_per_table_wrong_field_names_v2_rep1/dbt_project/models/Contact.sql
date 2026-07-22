{{ config(materialized='table') }}

SELECT 
    ap_id AS "Id",
    TRIM(vorname) AS "FirstName",
    TRIM(nachname) AS "LastName",
    TRIM(email_adresse) AS "Email",
    TRIM(telefonnummer) AS "Phone",
    TRIM(position) AS "Title",
    CASE 
        WHEN TRIM(funktion) = 'Decision Maker' THEN 'Decision Maker'
        WHEN TRIM(funktion) = 'End User' THEN 'End User'
        WHEN TRIM(funktion) = 'Technical Contact' THEN 'Technical Contact'
        WHEN TRIM(funktion) = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(sprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(sprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(sprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(sprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(sprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunden."Id" AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} 
LEFT JOIN (
    SELECT 
        kunden_nr AS "Legacy_Customer_ID__c",
        kunden_nr AS "Id"
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
) AS kunden 
    ON TRIM(kunde) = TRIM(kunden."Legacy_Customer_ID__c")