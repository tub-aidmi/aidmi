{{ config(materialized='table') }}

SELECT
    '003' || MD5(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    TRIM(ap.nachname) AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE 
        WHEN UPPER(ap.funktion) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(ap.funktion) = 'END USER' THEN 'End User'
        WHEN UPPER(ap.funktion) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN UPPER(ap.funktion) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(ap.sprache) = 'DE' THEN 'DE'
        WHEN UPPER(ap.sprache) = 'EN' THEN 'EN'
        WHEN UPPER(ap.sprache) = 'FR' THEN 'FR'
        WHEN UPPER(ap.sprache) = 'ES' THEN 'ES'
        WHEN UPPER(ap.sprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || MD5(k.kunden_nr) AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON ap.kunde = k.kunden_nr
