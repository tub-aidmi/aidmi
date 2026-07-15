{{ config(materialized='table') }}

SELECT
    -- Generate deterministic Salesforce-style 18-char Id from proj_id
    CONCAT(
        'a00',                                    -- custom object prefix
        LEFT(SUBSTRING(MD5(p.proj_id), 1, 12), 12) -- hash chars
    ) AS "Id",
    
    -- Project name (NOT NULL in target)
    INITCAP(TRIM(p.name)) AS "Name",
    
    -- Status maps directly (source values already match target enum)
    CASE p.status
        WHEN 'Active' THEN 'Active'
        WHEN 'Completed' THEN 'Completed'
        WHEN 'In Planning' THEN 'In Planning'
        WHEN 'On Hold' THEN 'On Hold'
        WHEN 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    
    -- Go-Live date (already ISO YYYY-MM-DD in source)
    p.go_live AS "Go_Live_Date__c",
    
    -- Account reference: transform kd using SAME hash as Account model would use for kunden_nr
    CONCAT(
        '001',                                    -- standard Account prefix
        LEFT(SUBSTRING(MD5(p.kd), 1, 12), 12)     -- same hash logic as in Account model
    ) AS "Account__c",
    
    -- Opportunity reference: transform opp using SAME hash as Opportunity model would use for chance_id
    CONCAT(
        '006',                                    -- standard Opportunity prefix
        LEFT(SUBSTRING(MD5(p.opp), 1, 12), 12)   -- same hash logic as in Opportunity model
    ) AS "Opportunity__c",
    
    -- Legacy ID from source natural key
    p.proj_id AS "Legacy_Project_ID__c",
    
    -- These don't exist in source; use NULL for dates, 0 for IsDeleted
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p