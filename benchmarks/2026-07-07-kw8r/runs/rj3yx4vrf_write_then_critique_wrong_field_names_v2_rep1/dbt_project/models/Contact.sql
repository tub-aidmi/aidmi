{{ config(materialized='table') }}

SELECT
     -- Generate SFDC-style Contact Id using MD5 hash of source key with 003 prefix
    '003' || SUBSTRING(MD5(ap.ap_id), 1, 15) AS "Id",
    
    ap.vorname AS "FirstName",
    
    COALESCE(NULLIF(TRIM(ap.nachname), ''), 'Unknown') AS "LastName",
    
    TRIM(ap.email_adresse) AS "Email",
    
    TRIM(ap.telefonnummer) AS "Phone",
    
    INITCAP(TRIM(ap.position)) AS "Title",
    
     -- Map funktion (German role field) to Salesforce Role__c enum
    CASE
        WHEN LOWER(TRIM(ap.funktion)) IN ('entscheider', 'decision maker', 'decisionmaker') THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap.funktion)) IN ('endanwender', 'end user', 'endverbraucher') THEN 'End User'
        WHEN LOWER(TRIM(ap.funktion)) IN ('technischer ansprechpartner', 'technical contact', 'tech contact') THEN 'Technical Contact'
        WHEN LOWER(TRIM(ap.funktion)) IN ('geschäftsführer', 'vorstand', 'ceo', 'executive sponsor', 'c-level') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    
     -- Map sprache (German language field) to Salesforce Preferred_Language__c enum
    CASE
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'GERMAN', 'DEU', 'GER') THEN 'DE'
        WHEN UPPER(TRIM(ap.sprache)) IN ('EN', 'ENGLISH', 'ENG', 'GBR', 'USA') THEN 'EN'
        WHEN UPPER(TRIM(ap.sprache)) IN ('FR', 'FRENCH', 'FRA', 'FRE') THEN 'FR'
        WHEN UPPER(TRIM(ap.sprache)) IN ('ES', 'SPANISH', 'ESP', 'SPN') THEN 'ES'
        WHEN UPPER(TRIM(ap.sprache)) IN ('IT', 'ITALIAN', 'ITA') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    
     -- AccountId: use the SAME transform as Account.Id so cross-table joins work
    CASE
        WHEN k.kunden_nr IS NOT NULL 
            THEN '001' || LPAD(TRIM(k.kunden_nr), 12, '0')
        ELSE NULL
    END AS "AccountId",
    
     -- Legacy contact ID from source natural key (trimmed for consistency)
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(LOWER(ap.kunde)) = TRIM(LOWER(k.kunden_nr))