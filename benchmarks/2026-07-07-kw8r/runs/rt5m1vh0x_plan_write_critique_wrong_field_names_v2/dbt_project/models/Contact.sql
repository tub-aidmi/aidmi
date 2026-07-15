{{ config(materialized='table') }}
SELECT 
    MD5('Contact_' || TRIM(ap.ap_id)) AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    INITCAP(TRIM(ap.nachname)) AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE 
        WHEN TRIM(ap.funktion) IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor') 
            THEN TRIM(ap.funktion)
        ELSE NULL 
    END AS "Role__c",
    CASE 
        WHEN TRIM(ap.sprache) IN ('DE', 'EN', 'FR', 'ES', 'IT') 
            THEN TRIM(ap.sprache)
        ELSE NULL 
    END AS "Preferred_Language__c",
    MD5(TRIM(k.kunden_nr)) AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    '1970-01-01' AS "CreatedDate",
    '1970-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(ap.kunde) = TRIM(k.kunden_nr)