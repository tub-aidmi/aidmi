{{ config(materialized='table') }}

SELECT
     -- Id: Use kontakt_id with canonical CON- prefix format
    TRIM(kontakt_id) AS "Id",
    
     -- FirstName: INITCAP of rufname, NULL if empty
    CASE 
        WHEN TRIM(rufname) IS NOT NULL AND TRIM(rufname) != '' THEN INITCAP(TRIM(rufname))
        ELSE NULL 
    END AS "FirstName",
    
     -- LastName: NOT NULL constraint — default to 'Unknown'
    COALESCE(NULLIF(INITCAP(TRIM(familienname)), ''), 'Unknown') AS "LastName",
    
     -- Email: lowercase trimmed email
    CASE 
        WHEN TRIM(kontakt_email) IS NOT NULL AND TRIM(kontakt_email) != '' THEN LOWER(TRIM(kontakt_email))
        ELSE NULL 
    END AS "Email",
    
     -- Phone: keep only digits and leading '+'
    CASE 
        WHEN TRIM(tel) IS NOT NULL AND TRIM(tel) != '' THEN REGEXP_REPLACE(TRIM(tel), '[^0-9+]', '')
        ELSE NULL 
    END AS "Phone",
    
     -- Title: INITCAP of berufsbezeichnung, NULL if empty
    CASE 
        WHEN TRIM(berufsbezeichnung) IS NOT NULL AND TRIM(berufsbezeichnung) != '' THEN INITCAP(TRIM(berufsbezeichnung))
        ELSE NULL 
    END AS "Title",
    
     -- Role__c: map German/English variants to enum domain
    CASE 
        WHEN LOWER(TRIM(rolle)) IN ('decision maker', 'dm', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(rolle)) IN ('end user', 'nutzer', 'benutzer', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(rolle)) IN ('technical contact', 'tech', 'technisch', 'technischer ansprechpartner', 'techniker') THEN 'Technical Contact'
        WHEN LOWER(TRIM(rolle)) IN ('executive sponsor', 'sponsor', 'gesellschafter') THEN 'Executive Sponsor'
        ELSE NULL 
    END AS "Role__c",
    
     -- Preferred_Language__c: map language names to ISO codes
    CASE 
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('EN', 'ENG', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('FR', 'FRA', 'FRENCH', 'FRANZÖSISCH') THEN 'FR'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('ES', 'ESP', 'SPANISH', 'ESPANOL', 'ESPANHOL') THEN 'ES'
        WHEN UPPER(TRIM(korrespondenzsprache)) IN ('IT', 'ITA', 'ITALIAN', 'ITALIANO') THEN 'IT'
        ELSE NULL 
    END AS "Preferred_Language__c",
    
     -- AccountId: canonicalize kd_nummer with same transform as Account.Id (CUS- prefix)
    CASE 
        WHEN TRIM(kd_nummer) IS NOT NULL AND TRIM(kd_nummer) != '' THEN 
            REGEXP_REPLACE(UPPER(TRIM(kd_nummer)), '^CUST-|^CUS-', 'CUS-')
        ELSE NULL 
    END AS "AccountId",
    
     -- Legacy_Contact_ID__c: direct copy of kontakt_id for row-level verification
    TRIM(kontakt_id) AS "Legacy_Contact_ID__c",
    
     -- CreatedDate: no source field; use constant default
     '2024-01-01' AS "CreatedDate",
    
     -- LastModifiedDate: no source field; mirror CreatedDate
     '2024-01-01' AS "LastModifiedDate",
    
     -- IsDeleted: no source field; default to 0 (not deleted)
     0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}