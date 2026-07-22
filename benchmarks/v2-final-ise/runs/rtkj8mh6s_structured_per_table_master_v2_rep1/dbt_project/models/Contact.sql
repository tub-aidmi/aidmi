{{ config(materialized='table') }}

SELECT 
    -- Contact Id: Salesforce-style ID generated from kontakt_id
    'CONT' || LPAD(REGEXP_REPLACE(UPPER(TRIM(k.kontakt_id)), '[^0-9]', ''), 10, '0') AS "Id",
    
    -- FirstName from rufname (German given name)
    TRIM(k.rufname) AS "FirstName",
    
    -- LastName from familienname (NOT NULL — default to empty string if missing)
    COALESCE(TRIM(k.familienname), '') AS "LastName",
    
    -- Email normalized to lowercase
    LOWER(TRIM(k.kontakt_email)) AS "Email",
    
    -- Phone number cleaned
    TRIM(k.tel) AS "Phone",
    
    -- Job title capitalized
    INITCAP(TRIM(k.berufsbezeichnung)) AS "Title",
    
    -- Role__c enum: map source rolle values to target enum domain
    CASE 
        WHEN LOWER(TRIM(k.rolle)) IN ('decision maker', 'dm') THEN 'Decision Maker'
        WHEN LOWER(TRIM(k.rolle)) IN ('end user', 'eu') THEN 'End User'
        WHEN LOWER(TRIM(k.rolle)) IN ('technical contact', 'tc') THEN 'Technical Contact'
        WHEN LOWER(TRIM(k.rolle)) IN ('executive sponsor', 'es') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    
    -- Preferred_Language__c enum: normalize source language code to target domain
    CASE 
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(k.korrespondenzsprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    
    -- AccountId: reference Salesforce-style Account Id derived from joined customer number
    -- Uses same key transform as Account.Id for cross-table consistency
    'ACCT' || LPAD(REGEXP_REPLACE(UPPER(TRIM(m.kundennummer)), '[^0-9]', ''), 10, '0') AS "AccountId",
    
    -- Legacy_Contact_ID__c: populated from source natural key
    TRIM(k.kontakt_id) AS "Legacy_Contact_ID__c",
    
    -- CreatedDate: no temporal source data available
    NULL AS "CreatedDate",
    
    -- LastModifiedDate: no temporal source data available
    NULL AS "LastModifiedDate",
    
    -- IsDeleted: not deleted (default)
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} k
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} m 
    ON TRIM(UPPER(k.kd_nummer)) = TRIM(UPPER(m.kundennummer))