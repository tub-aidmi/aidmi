{{ config(materialized='table') }}

-- Installed_Asset model: parse warranty date, resolve cross-table keys to Account and Project
with source as (
    select *
    from {{ source('fixture_missing_relations_v2_src', 'asset') }}
),

parsed_date as (
    select *,
        -- Parse warranty date from multiple possible formats into ISO YYYY-MM-DD
        CASE
            WHEN "warranty" IS NULL OR TRIM("warranty") = '' THEN NULL

            -- YYYY-MM-DD format (10 chars, has dashes)
            WHEN LENGTH(TRIM("warranty")) = 10 AND TRIM("warranty") ~ '^\d{4}-\d{2}-\d{2}$'
                THEN TO_DATE(TRIM("warranty"), 'YYYY-MM-DD')::TEXT

            -- DD.MM.YYYY format (European, has dots)
            WHEN TRIM("warranty") ~ '^\d{2}\.\d{2}\.\d{4}$'
                THEN TO_DATE(TRIM("warranty"), 'DD.MM.YYYY')::TEXT

            -- MM/DD/YYYY format (US, has slashes and is 10 chars)
            WHEN LENGTH(TRIM("warranty")) = 10 AND TRIM("warranty") ~ '^\d{2}/\d{2}/\d{4}$'
                THEN TO_DATE(TRIM("warranty"), 'MM/DD/YYYY')::TEXT

            -- YYYYMMDD format (no separators, 8 digits)
            WHEN LENGTH(TRIM("warranty")) = 8 AND TRIM("warranty") ~ '^\d{8}$'
                THEN TO_DATE(TRIM("warranty"), 'YYYYMMDD')::TEXT

            ELSE NULL
        END AS "parsed_warranty"

    from source
)

select
    -- Id: asset primary key
    "id" AS "Id",

    -- Name: required, fallback to 'Unknown' if empty
    CASE
        WHEN COALESCE(TRIM("name"), '') = '' THEN 'Unknown'
        ELSE TRIM("name")
    END AS "Name",

    -- Serial_Number__c
    TRIM(COALESCE("serial", '')) AS "Serial_Number__c",

    -- Warranty_End_Date__c: parsed date in ISO format
    CASE
        WHEN COALESCE(TRIM("warranty"), '') = '' THEN NULL
        ELSE "parsed_warranty"
    END AS "Warranty_End_Date__c",

    -- Account__c: map asset.client to account id format
    CASE
        WHEN COALESCE(TRIM("client"), '') = '' THEN NULL
        ELSE TRIM("client")
    END AS "Account__c",

    -- Project__c: map asset.project to project id format
    CASE
        WHEN COALESCE(TRIM("project"), '') = '' THEN NULL
        ELSE TRIM("project")
    END AS "Project__c",

    -- Legacy_Asset_ID__c: from source natural key
    "id" AS "Legacy_Asset_ID__c",

    -- CreatedDate: not in source, default NULL
    NULL AS "CreatedDate",

    -- LastModifiedDate: not in source, default NULL
    NULL AS "LastModifiedDate",

    -- IsDeleted: default 0 (active)
    0 AS "IsDeleted"

from parsed_date
