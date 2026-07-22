{{ config(materialized='table') }}

SELECT 
    -- Generate Salesforce-style Project Id from source PROJ-XXXXX natural key
    CASE 
        WHEN id IS NOT NULL AND TRIM(id) != '' THEN
            '00P' || LPAD(
                CASE 
                    WHEN REGEXP_REPLACE(TRIM(id), '[^0-9]', '') ~ '^\d+$' 
                    THEN CAST(REGEXP_REPLACE(TRIM(id), '[^0-9]', '') AS INTEGER)
                    ELSE 0
                END::TEXT, 
                12, '0'
            )
        ELSE '00P000000000000'
    END AS "Id",

    -- COALESCE prevents NOT NULL constraint violation on Name
    COALESCE(name, 'Unnamed Project') AS "Name",

    -- Map project status to enum domain, normalizing case and spelling
    CASE 
        WHEN LOWER(TRIM(project_status__c)) IN ('aktiv', 'active') THEN 'Active'
        WHEN LOWER(TRIM(project_status__c)) IN ('abgeschlossen', 'completed', 'close') THEN 'Completed'
        WHEN LOWER(TRIM(project_status__c)) IN ('planung', 'in planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(TRIM(project_status__c)) IN ('on hold', 'pausiert', 'paused', 'gepaust') THEN 'On Hold'
        WHEN LOWER(TRIM(project_status__c)) IN ('storniert', 'cancelled', 'storno', 'stornert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",

    -- Parse go_live_date from multiple formats, output ISO YYYY-MM-DD or NULL
    CASE 
        WHEN go_live_date__c IS NULL OR TRIM(go_live_date__c) = '' THEN NULL
        WHEN TRIM(go_live_date__c) IN ('N/A', 'n/a', '-', '0000-00-00') THEN NULL
        WHEN TRIM(go_live_date__c) ~ '^\d{8}$' THEN TO_DATE(TRIM(go_live_date__c), 'YYYYMMDD')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(go_live_date__c), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(go_live_date__c) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(go_live_date__c), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",

    -- Transform source CUST-XXXX into Salesforce-style Account Id (001 prefix)
    CASE 
        WHEN account__c IS NOT NULL AND TRIM(account__c) != '' THEN
            '001' || LPAD(
                CASE 
                    WHEN REGEXP_REPLACE(TRIM(account__c), '[^0-9]', '') ~ '^\d+$' 
                    THEN CAST(REGEXP_REPLACE(TRIM(account__c), '[^0-9]', '') AS INTEGER)
                    ELSE 0
                END::TEXT, 
                12, '0'
            )
        ELSE NULL
    END AS "Account__c",

    -- Transform source OPP-XXXXX into Salesforce-style Opportunity Id (006 prefix)
    CASE 
        WHEN opportunity__c IS NOT NULL AND TRIM(opportunity__c) != '' THEN
            '006' || LPAD(
                CASE 
                    WHEN REGEXP_REPLACE(TRIM(opportunity__c), '[^0-9]', '') ~ '^\d+$' 
                    THEN CAST(REGEXP_REPLACE(TRIM(opportunity__c), '[^0-9]', '') AS INTEGER)
                    ELSE 0
                END::TEXT, 
                12, '0'
            )
        ELSE NULL
    END AS "Opportunity__c",

    -- Legacy Project ID from source natural key (kept for row-level verification)
    id AS "Legacy_Project_ID__c",

    CAST(NOW() AS TEXT) AS "CreatedDate",
    CAST(NOW() AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}