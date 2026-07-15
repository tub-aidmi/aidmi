{{ config(materialized='table') }}

-- Project model: parse go_live date, map status, resolve cross-table keys
with source as (
    select *
    from {{ source('fixture_missing_relations_v2_src', 'project') }}
),

parsed_date as (
    select *,
        -- Parse go_live date from multiple possible formats into ISO YYYY-MM-DD
        CASE
            WHEN "go_live" IS NULL OR TRIM("go_live") = '' THEN NULL

            -- YYYY-MM-DD format (10 chars, has dashes)
            WHEN LENGTH(TRIM("go_live")) = 10 AND TRIM("go_live") ~ '^\d{4}-\d{2}-\d{2}$'
                THEN TO_DATE(TRIM("go_live"), 'YYYY-MM-DD')::TEXT

            -- DD.MM.YYYY format (European, has dots)
            WHEN TRIM("go_live") ~ '^\d{2}\.\d{2}\.\d{4}$'
                THEN TO_DATE(TRIM("go_live"), 'DD.MM.YYYY')::TEXT

            -- MM/DD/YYYY format (US, has slashes and is 10 chars)
            WHEN LENGTH(TRIM("go_live")) = 10 AND TRIM("go_live") ~ '^\d{2}/\d{2}/\d{4}$'
                THEN TO_DATE(TRIM("go_live"), 'MM/DD/YYYY')::TEXT

            -- YYYYMMDD format (no separators, 8 digits)
            WHEN LENGTH(TRIM("go_live")) = 8 AND TRIM("go_live") ~ '^\d{8}$'
                THEN TO_DATE(TRIM("go_live"), 'YYYYMMDD')::TEXT

            ELSE NULL
        END AS "parsed_go_live"

    from source
)

select
    -- Id: project primary key
    "id" AS "Id",

    -- Name: required, fallback to 'Unknown' if empty
    CASE
        WHEN COALESCE(TRIM("name"), '') = '' THEN 'Unknown'
        ELSE TRIM("name")
    END AS "Name",

    -- Project_Status__c: map source status values to allowed enum
    CASE UPPER(TRIM(COALESCE("status", '')))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        WHEN 'CLOSED' THEN 'Completed'
        WHEN 'IN PROGRESS' THEN 'Active'
        WHEN 'PLANNED' THEN 'In Planning'
        WHEN 'PAUSED' THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",

    -- Go_Live_Date__c: parsed date in ISO format
    CASE
        WHEN COALESCE(TRIM("go_live"), '') = '' THEN NULL
        ELSE "parsed_go_live"
    END AS "Go_Live_Date__c",

    -- Account__c: map project.client_id to account id format
    CASE
        WHEN COALESCE(TRIM("client_id"), '') = '' THEN NULL
        ELSE TRIM("client_id")
    END AS "Account__c",

    -- Opportunity__c: map project.opportunity_ref to opportunity id format
    CASE
        WHEN COALESCE(TRIM("opportunity_ref"), '') = '' THEN NULL
        ELSE TRIM("opportunity_ref")
    END AS "Opportunity__c",

    -- Legacy_Project_ID__c: from source natural key
    "id" AS "Legacy_Project_ID__c",

    -- CreatedDate: not in source, default NULL
    NULL AS "CreatedDate",

    -- LastModifiedDate: not in source, default NULL
    NULL AS "LastModifiedDate",

    -- IsDeleted: default 0 (active)
    0 AS "IsDeleted"

from parsed_date
