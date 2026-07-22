{{ config(materialized='table') }}

SELECT
    -- Id: Strip 'PROJ-' prefix, add 'PROJ' prefix for Salesforce-style ID
    'PROJ' || REGEXP_REPLACE(TRIM(p.proj_id), '^PROJ-?', '', 'i') AS "Id",

    -- Name: TRIM with fallback; NOT NULL enforced
    CASE 
        WHEN TRIM(p.name) IS NULL OR TRIM(p.name) = '' THEN 'Unnamed Project'
        ELSE TRIM(p.name)
    END AS "Name",

    -- Project_Status__c: Source already in English enum values
    CASE INITCAP(TRIM(p.status))
        WHEN 'Active' THEN 'Active'
        WHEN 'Completed' THEN 'Completed'
        WHEN 'In Planning' THEN 'In Planning'
        WHEN 'On Hold' THEN 'On Hold'
        WHEN 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",

    -- Go_Live_Date__c: Parse ISO (YYYY-MM-DD) first, fallback to DD.MM.YYYY; NULL on failure
    CASE 
        WHEN TRIM(p.go_live) IS NULL THEN NULL
        WHEN TRIM(p.go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(p.go_live), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(p.go_live) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",

    -- Account__c: FK to Account via kunden join; transform source CUS prefix to Salesforce format
    CASE WHEN k.kunden_nr IS NOT NULL 
        THEN 'CUS' || REGEXP_REPLACE(TRIM(k.kunden_nr), '^CUST-?', '', 'i')
        ELSE NULL
    END AS "Account__c",

    -- Opportunity__c: FK to Opportunity via chancen join; transform source OPP prefix
    CASE WHEN c.chance_id IS NOT NULL
        THEN 'OPP' || REGEXP_REPLACE(TRIM(c.chance_id), '^OPP-?', '', 'i')
        ELSE NULL
    END AS "Opportunity__c",

    -- Legacy_Project_ID__c: Direct passthrough of source proj_id
    TRIM(p.proj_id) AS "Legacy_Project_ID__c",

    -- Audit placeholders (source has no timestamps)
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p

-- Resolve Account FK: proj.kd → kunden.kunden_nr (both use CUST-NNNN format)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(p.kd) = TRIM(k.kunden_nr)

-- Resolve Opportunity FK: proj.opp → chancen.chance_id (both use OPP-NNNNN format)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c 
    ON TRIM(p.opp) = TRIM(c.chance_id)