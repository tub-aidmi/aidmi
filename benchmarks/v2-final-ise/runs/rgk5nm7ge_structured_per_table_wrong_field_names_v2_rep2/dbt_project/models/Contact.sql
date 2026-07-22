{{ config(materialized='table') }}

SELECT
    -- Primary key for Contact - use ap_id as Salesforce-style ID
    a.ap_id AS "Id",
    
    -- First name from vorname
    a.vorname AS "FirstName",
    
    -- Last name from nachname, default to 'Unknown' if null/empty (NOT NULL constraint)
    COALESCE(NULLIF(a.nachname, ''), 'Unknown') AS "LastName",
    
    -- Email from email_adresse
    a.email_adresse AS "Email",
    
    -- Phone from telefonnummer
    a.telefonnummer AS "Phone",
    
    -- Title/Position from position
    a.position AS "Title",
    
    -- Role mapping from funktion to Salesforce role enum
    CASE 
        WHEN LOWER(TRIM(a.funktion)) IN ('decision maker', 'end user', 'technical contact', 'executive sponsor') THEN INITCAP(TRIM(a.funktion))
        ELSE NULL
    END AS "Role__c",
    
    -- Preferred language from sprache (DE, EN, FR)
    CASE 
        WHEN UPPER(TRIM(a.sprache)) IN ('DE', 'EN', 'FR') THEN UPPER(TRIM(a.sprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    
    -- AccountId: lookup from kunden table to join with Account (both use CUST-XXXX format)
    k.kunden_nr AS "AccountId",
    
    -- Legacy Contact ID - same as ap_id for verification
    a.ap_id AS "Legacy_Contact_ID__c",
    
    -- No source timestamp fields available, set NULL
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    
    -- Default to not deleted
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON UPPER(TRIM(a.kunde)) = UPPER(TRIM(k.kunden_nr))