{{ config(materialized='table') }}

SELECT 
    -- Contact primary key from source natural key
    CAST(a.ap_id AS TEXT) AS "Id",
    
    -- First name
    NULLIF(TRIM(a.vorname), '') AS "FirstName",
    
    -- Last name (NOT NULL – default to 'Unknown' if missing)
    COALESCE(NULLIF(TRIM(a.nachname), ''), 'Unknown') AS "LastName",
    
    -- Email
    NULLIF(TRIM(a.email_adresse), '') AS "Email",
    
    -- Phone
    NULLIF(TRIM(a.telefonnummer), '') AS "Phone",
    
    -- Title / position
    NULLIF(TRIM(a.position), '') AS "Title",
    
    -- Role: map German funktion values to target enum domain
    CASE UPPER(TRIM(a.funktion))
        WHEN 'ENTSCHEIDER'          THEN 'Decision Maker'
        WHEN 'MANAGER'              THEN 'Decision Maker'
        WHEN 'LEITER'               THEN 'Decision Maker'
        WHEN 'GESCHÄFTSFÜHRER'      THEN 'Executive Sponsor'
        WHEN 'INHABER'              THEN 'Executive Sponsor'
        WHEN 'CEO'                  THEN 'Executive Sponsor'
        WHEN 'CFO'                  THEN 'Executive Sponsor'
        WHEN 'TECHNIKER'            THEN 'Technical Contact'
        WHEN 'IT-SUPPORT'           THEN 'Technical Contact'
        WHEN 'SUPPORT'              THEN 'Technical Contact'
        WHEN 'END USER'             THEN 'End User'
        ELSE NULL
    END AS "Role__c",
    
    -- Preferred language: map German language names to ISO codes (DE/EN/FR/ES/IT)
    CASE UPPER(TRIM(a.sprache))
        WHEN 'DE'           THEN 'DE'
        WHEN 'EN'           THEN 'EN'
        WHEN 'FR'           THEN 'FR'
        WHEN 'ES'           THEN 'ES'
        WHEN 'IT'           THEN 'IT'
        WHEN 'DEUTSCH'      THEN 'DE'
        WHEN 'ENGLISCH'     THEN 'EN'
        WHEN 'ENGLISH'      THEN 'EN'
        WHEN 'FRANZÖSISCH'  THEN 'FR'
        WHEN 'FRENCH'       THEN 'FR'
        WHEN 'SPANISCH'     THEN 'ES'
        WHEN 'SPANISH'      THEN 'ES'
        WHEN 'ITALIENISCH'  THEN 'IT'
        WHEN 'ITALIAN'      THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    
    -- AccountId: resolve via join to customers table; use Salesforce-style Id with '001' prefix
    '001' || TRIM(k.kunden_nr) AS "AccountId",
    
    -- Legacy contact ID from source natural key (for row-level verification)
    a.ap_id AS "Legacy_Contact_ID__c",
    
    -- No explicit created/modified date columns in this source table
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(a.kunde) = TRIM(k.kunden_nr)