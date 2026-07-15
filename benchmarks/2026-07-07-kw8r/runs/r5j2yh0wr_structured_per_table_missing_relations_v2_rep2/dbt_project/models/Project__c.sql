{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('fixture_missing_relations_v2_src', 'project') }}
),

parsed AS (
    SELECT
        id,
        name,
        status,
        go_live,
        client_id,
        opportunity_ref,
        -- Parse date from multiple formats to ISO YYYY-MM-DD
        CASE
            WHEN go_live IS NULL OR TRIM(go_live) = '' THEN NULL
            WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live  -- YYYY-MM-DD
            WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(go_live, 'DD.MM.YYYY')::TEXT  -- DD.MM.YYYY -> ISO
            WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(go_live, 'MM/DD/YYYY')::TEXT  -- MM/DD/YYYY -> ISO
            ELSE NULL
        END AS go_live_parsed,
        -- Map status to target enum with case normalization
        CASE INITCAP(TRIM(status))
            WHEN 'Active' THEN 'Active'
            WHEN 'Completed' THEN 'Completed'
            WHEN 'In Planning' THEN 'In Planning'
            WHEN 'On Hold' THEN 'On Hold'
            WHEN 'Cancelled' THEN 'Cancelled'
            WHEN 'Complete' THEN 'Completed'  -- synonym handling
            WHEN 'Planned' THEN 'In Planning'  -- reasonable default
            ELSE NULL  -- fallback NULL for unmapped values
        END AS status_mapped
    FROM source
)

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unnamed Project') AS "Name",
    status_mapped AS "Project_Status__c",
    go_live_parsed AS "Go_Live_Date__c",
    client_id AS "Account__c",
    opportunity_ref AS "Opportunity__c",
    id AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM parsed;
