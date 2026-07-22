{{ config(materialized='table') }}

WITH project_data AS (
    SELECT
        -- Id: Salesforce-style Id derived from source key
        'PROJ-' || SUBSTRING(TRIM(p.projekt_kennung) FROM 6) AS "Id",
        
        -- Name: project name, trimmed and title-cased
        INITCAP(TRIM(p.projektname)) AS "Name",
        
        -- Project_Status__c: mapped from various German/English status values
        CASE LOWER(TRIM(p.projektstatus))
            WHEN 'active' THEN 'Active'
            WHEN 'aktiv' THEN 'Active'
            WHEN 'completed' THEN 'Completed'
            WHEN 'abgeschlossen' THEN 'Completed'
            WHEN 'in planning' THEN 'In Planning'
            WHEN 'in planung' THEN 'In Planning'
            WHEN 'planung' THEN 'In Planning'
            WHEN 'on hold' THEN 'On Hold'
            WHEN 'pausiert' THEN 'On Hold'
            WHEN 'cancelled' THEN 'Cancelled'
            WHEN 'storniert' THEN 'Cancelled'
            ELSE NULL
        END AS "Project_Status__c",
        
        -- Go_Live_Date__c: parse multiple date formats to ISO YYYY-MM-DD
        CASE
            -- Invalid sentinel date
            WHEN p.go_live_datum = '0000-00-00' THEN NULL
            -- Already ISO format
            WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            -- European format DD.MM.YYYY
            WHEN p.go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            -- US format M/D/YYYY or MM/DD/YYYY
            WHEN p.go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            -- Compact YYYYMMDD format
            WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS "Go_Live_Date__c",
        
        -- Account__c: reference to Salesforce Account Id (derived from kundennummer)
        CASE 
            WHEN p.kunden_kennung IS NOT NULL THEN
                'CUST-' || SUBSTRING(TRIM(p.kunden_kennung) FROM 6)
            ELSE NULL
        END AS "Account__c",
        
        -- Opportunity__c: reference to Salesforce Opportunity Id (derived from opp_kennung_ref)
        CASE 
            WHEN p.opp_kennung_ref IS NOT NULL THEN
                TRIM(p.opp_kennung_ref)
            ELSE NULL
        END AS "Opportunity__c",
        
        -- Legacy_Project_ID__c: raw source project key
        TRIM(p.projekt_kennung) AS "Legacy_Project_ID__c",
        
        -- Audit columns (not in source, use reasonable defaults)
        CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
        CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"
    
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p
    
    -- Filter out rows with completely null project names (data quality)
    WHERE p.projektname IS NOT NULL AND TRIM(p.projektname) != ''
)

SELECT * FROM project_data