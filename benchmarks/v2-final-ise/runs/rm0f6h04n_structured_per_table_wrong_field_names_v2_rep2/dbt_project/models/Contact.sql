{{ config(materialized='table') }}

SELECT 
    -- Salesforce-style Contact ID (003 prefix)
    CONCAT('003', SUBSTRING(MD5(ap_id)::varchar FROM 1 FOR 15)) AS "Id",
    
    -- FirstName: trim, capitalize first letters of words
    INITCAP(TRIM(vorname)) AS "FirstName",
    
    -- LastName: trim and normalize; default to 'Unknown' if null (NOT NULL target column)
    COALESCE(INITCAP(TRIM(nachname)), 'Unknown') AS "LastName",
    
    -- Email: lowercase and trim for consistency
    LOWER(TRIM(email_adresse)) AS "Email",
    
    -- Phone: clean up whitespace
    TRIM(telefonnummer) AS "Phone",
    
    -- Title/Position: capitalize properly
    INITCAP(TRIM(position)) AS "Title",
    
    -- Role__c: map German function titles to Salesforce roles
    CASE 
        WHEN UPPER(funktion) LIKE '%ENTSCHEIDER%' 
          OR UPPER(funktion) LIKE '%LEITUNG%' 
          OR UPPER(funktion) LIKE '%CHEF%' 
          OR UPPER(funktion) LIKE '%MANAGER%' THEN 'Decision Maker'
        WHEN UPPER(funktion) LIKE '%TECHNIK%' 
          OR UPPER(funktion) LIKE '%SUPPORT%' 
          OR UPPER(funktion) LIKE '%ADMIN%' THEN 'Technical Contact'
        WHEN UPPER(funktion) LIKE '%SPONSOR%' 
          OR UPPER(funktion) LIKE '%VORSITZENDER%' 
          OR UPPER(funktion) LIKE '%PRESIDENT%' THEN 'Executive Sponsor'
        ELSE 'End User'
    END AS "Role__c",
    
    -- Preferred_Language__c: normalize language names/codes to 2-letter ISO codes
    CASE 
        WHEN UPPER(TRIM(sprache)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(sprache)) IN ('EN', 'ENGLISCH', 'ENGLISH') THEN 'EN'
        WHEN UPPER(TRIM(sprache)) IN ('FR', 'FRANZOESISCHE', 'FRENCH') THEN 'FR'
        WHEN UPPER(TRIM(sprache)) IN ('ES', 'SPANISCH', 'SPANISH') THEN 'ES'
        WHEN UPPER(TRIM(sprache)) IN ('IT', 'ITALIENISCH', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    
    -- AccountId: transform customer number to Salesforce-style Account ID (001 prefix)
    -- Uses same deterministic hash as the Account model so keys resolve correctly
    CONCAT('001', SUBSTRING(MD5(kunde)::varchar FROM 1 FOR 15)) AS "AccountId",
    
    -- Legacy contact ID from source primary key
    ap_id AS "Legacy_Contact_ID__c",
    
    -- Default dates (no source timestamps available)
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    
    -- Not deleted by default
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}