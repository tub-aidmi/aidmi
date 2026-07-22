{{ config(materialized='table') }}

SELECT
    -- Id: Salesforce-style 18-char custom object ID from proj_id (prefix a1x + zero-padded number)
    CONCAT(
        'a1x',
        LPAD(SUBSTRING(p.proj_id FROM E'\\d+'), 15, '0')
    ) AS "Id",

    -- Name: normalized company name with fallback
    COALESCE(NULLIF(TRIM(p.name), ''), 'Unnamed Project') AS "Name",

    -- Project_Status__c: map source status values to target enum; fallback 'In Planning'
    CASE
        WHEN UPPER(TRIM(p.status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(p.status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(p.status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(p.status)) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM(p.status)) = 'CANCELLED' THEN 'Cancelled'
        ELSE 'In Planning'
    END AS "Project_Status__c",

    -- Go_Live_Date__c: parse dates (already YYYY-MM-DD in source, but handle other formats)
    CASE
        WHEN p.go_live IS NULL OR TRIM(p.go_live) = '' THEN NULL
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{8}$' THEN
            SUBSTRING(p.go_live, 1, 4) || '-' ||
            SUBSTRING(p.go_live, 5, 2) || '-' ||
            SUBSTRING(p.go_live, 7, 2)
        ELSE NULL
    END AS "Go_Live_Date__c",

    -- Account__c: Salesforce Account ID derived from joined kunden.kunden_nr (prefix 001)
    CONCAT(
        '001',
        LPAD(SUBSTRING(k.kunden_nr FROM E'\\d+'), 14, '0')
    ) AS "Account__c",

    -- Opportunity__c: Salesforce Opportunity ID derived from joined chancen.chance_id (prefix 006)
    CONCAT(
        '006',
        LPAD(SUBSTRING(c.chance_id FROM E'\\d+'), 14, '0')
    ) AS "Opportunity__c",

    -- Legacy_Project_ID__c: raw source key for traceability
    TRIM(p.proj_id) AS "Legacy_Project_ID__c",

    -- Audit fields (not in source — deterministic defaults)
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(p.kd) = TRIM(k.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
    ON TRIM(p.opp) = TRIM(c.chance_id)