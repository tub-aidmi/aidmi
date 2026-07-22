{{ config(materialized='table') }}

SELECT 
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    COALESCE(ap.nachname, '') AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE 
        WHEN ap.funktion = 'Decision Maker' THEN 'Decision Maker'
        WHEN ap.funktion = 'End User' THEN 'End User'
        WHEN ap.funktion = 'Technical Contact' THEN 'Technical Contact'
        WHEN ap.funktion = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL 
    END AS "Role__c",
    CASE 
        WHEN ap.sprache IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN ap.sprache
        ELSE NULL 
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON ap.kunde = k.kunden_nr