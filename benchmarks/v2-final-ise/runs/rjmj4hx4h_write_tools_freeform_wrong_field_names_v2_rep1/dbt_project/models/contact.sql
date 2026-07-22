{{ config(materialized='table') }}

SELECT 
    '003' || SUBSTRING(MD5(ap_id), 1, 14) AS "Id",
    INITCAP(TRIM(vorname)) AS "FirstName",
    INITCAP(TRIM(nachname)) AS "LastName",
    LOWER(TRIM(email_adresse)) AS "Email",
    telefonnummer AS "Phone",
    INITCAP(TRIM(position)) AS "Title",
    CASE 
        WHEN TRIM(funktion) = 'Decision Maker' THEN 'Decision Maker'
        WHEN TRIM(funktion) = 'End User' THEN 'End User'
        WHEN TRIM(funktion) = 'Executive Sponsor' THEN 'Executive Sponsor'
        WHEN TRIM(funktion) = 'Technical Contact' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(sprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(sprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || SUBSTRING(MD5(k.kunden_nr), 1, 14) AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} a
JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kunde = k.kunden_nr
