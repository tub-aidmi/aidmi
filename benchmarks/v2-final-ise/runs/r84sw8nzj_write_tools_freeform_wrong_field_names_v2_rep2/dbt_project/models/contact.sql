{{ config(materialized='table') }}

SELECT
    '003' || LPAD(REGEXP_REPLACE(ap_id, '\D', '', 'g'), 12, '0') AS "Id",
    INITCAP(vorname) AS "FirstName",
    INITCAP(nachname) AS "LastName",
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE funktion
        WHEN 'Decision Maker' THEN 'Decision Maker'
        WHEN 'End User' THEN 'End User'
        WHEN 'Executive Sponsor' THEN 'Executive Sponsor'
        WHEN 'Technical Contact' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE sprache
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || LPAD(REGEXP_REPLACE(kunde, '\D', '', 'g'), 12, '0') AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
