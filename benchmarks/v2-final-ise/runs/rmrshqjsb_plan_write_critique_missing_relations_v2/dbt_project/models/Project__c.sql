{{ config(materialized='table') }}

SELECT 
    CAST(id AS TEXT) AS "Id",
    COALESCE(NULLIF(TRIM(INITCAP(name)), ''), 'Unnamed Project') AS "Name",
    CASE UPPER(TRIM(status))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live IS NULL OR TRIM(go_live) = '' THEN NULL
        WHEN TRIM(go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(go_live), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(go_live) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(go_live), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(go_live) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(go_live), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(go_live) ~ '^\d{8}$' THEN TO_DATE(TRIM(go_live), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    -- Normalize client_id to match Account.Id format: strip prefix/suffix, trim, upper
    UPPER(TRIM(CASE 
        WHEN TRIM(client_id) ~ '^ACCT-' THEN REGEXP_REPLACE(TRIM(client_id), '^ACCT-', '')
        WHEN TRIM(client_id) ~ '^CUST'  THEN REGEXP_REPLACE(TRIM(client_id), '^CUST', '')
        ELSE TRIM(client_id)
    END)) AS "Account__c",
    -- Normalize opportunity_ref to match Opportunity.Id format: strip prefix/suffix, trim, upper  
    UPPER(TRIM(CASE 
        WHEN TRIM(opportunity_ref) ~ '^OPP-' THEN REGEXP_REPLACE(TRIM(opportunity_ref), '^OPP-', '')
        WHEN TRIM(opportunity_ref) ~ '^OPTY'  THEN REGEXP_REPLACE(TRIM(opportunity_ref), '^OPTY', '')
        ELSE TRIM(opportunity_ref)
    END)) AS "Opportunity__c",
    CAST(id AS TEXT) AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }}