{{ config(materialized='table') }}

SELECT
    p.id                                           AS "Id",
    COALESCE(TRIM(p.name), 'Unknown')              AS "Name",
    CASE
        WHEN LOWER(TRIM(p.status)) = 'active'       THEN 'Active'
        WHEN LOWER(TRIM(p.status)) = 'completed'    THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) = 'in planning'  THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) = 'on hold'      THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) = 'cancelled'    THEN 'Cancelled'
        ELSE NULL
    END                                            AS "Project_Status__c",

    -- Multi-format date parsing: YYYY-MM-DD, DD.MM.YYYY, YYYYMMDD
    CASE
        WHEN TRIM(p.go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(p.go_live), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(p.go_live) ~ '^\d{2}\.\d{2}\.\d{4}$'  THEN TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(p.go_live) ~ '^\d{8}$'                THEN TO_DATE(TRIM(p.go_live), 'YYYYMMDD')::TEXT
        ELSE NULL
    END                                            AS "Go_Live_Date__c",

    -- Transform potential KD- prefix to ACC- for referential integrity with Account.Id
    REGEXP_REPLACE(TRIM(p.client_id), '^KD-', 'ACC-')  AS "Account__c",

    -- opportunity_ref already uses OPP- prefix matching Opportunity.Id format
    TRIM(p.opportunity_ref)                        AS "Opportunity__c",

    p.id                                           AS "Legacy_Project_ID__c",
    NULL::TEXT                                     AS "CreatedDate",
    NULL::TEXT                                     AS "LastModifiedDate",
    0                                              AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'project') }} p