{{ config(materialized='table') }}

SELECT
    -- Salesforce-style Id: prefix with "P" to distinguish from other entities
    'P' || proj.projekt_kennung AS "Id",
    
    -- Project Name (fallback to unknown if null)
    COALESCE(proj.projektname, 'Unnamed Project') AS "Name",
    
    -- Status mapping from German source values to English target enum
    CASE LOWER(TRIM(proj.projektstatus))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'in aktivität' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'erfolgreich' THEN 'Completed'
        WHEN 'fertig' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'geplant' THEN 'In Planning'
        WHEN 'paused' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'auf eis' THEN 'On Hold'
        WHEN 'hold' THEN 'On Hold'
        WHEN 'storniert' THEN 'Cancelled'
        WHEN 'gescheitert' THEN 'Cancelled'
        WHEN 'fehlgeschlagen' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    
    -- Go-Live Date: parse DD.MM.YYYY format common in German sources
    CASE 
        WHEN proj.go_live_datum IS NULL THEN NULL
        WHEN proj.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(proj.go_live_datum, 'DD.MM.YYYY')::TEXT
        WHEN proj.go_live_datum ~ '^\d{8}$' THEN 
            SUBSTR(proj.go_live_datum, 5, 2) || '-' || SUBSTR(proj.go_live_datum, 7, 2) || '-' || SUBSTR(proj.go_live_datum, 1, 4)
        WHEN proj.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN 
            SUBSTR(proj.go_live_datum, 7, 4) || '-' || SUBSTR(proj.go_live_datum, 1, 2) || '-' || SUBSTR(proj.go_live_datum, 4, 2)
        ELSE NULL
    END AS "Go_Live_Date__c",
    
    -- Account__c: transform customer key to match Account.Id format
    -- Assuming Account.Id uses prefix 'A' and customer keys from kunden_kennung need transformation
    -- Transform by prepending 'A' and removing any existing customer prefixes
    CASE 
        WHEN proj.kunden_kennung IS NULL THEN NULL
        ELSE 'A' || REGEXP_REPLACE(proj.kunden_kennung, '^(KUN|CUST|CUSTOMER)', '', 'i')
    END AS "Account__c",
    
    -- Opportunity__c: transform opportunity key reference to match Opportunity.Id format
    CASE 
        WHEN proj.opp_kennung_ref IS NULL THEN NULL
        ELSE 'O' || REGEXP_REPLACE(proj.opp_kennung_ref, '^(OPP|OP)', '', 'i')
    END AS "Opportunity__c",
    
    -- Legacy Project ID: raw source key
    proj.projekt_kennung AS "Legacy_Project_ID__c",
    
    -- Audit dates (no source data available; use placeholder NULL)
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    
    -- Not deleted by default
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }} proj