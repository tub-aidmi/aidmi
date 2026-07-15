{{ config(materialized='table') }}

with source as (
    select *
    from {{ source('fixture_missing_relations_v2_src', 'account') }}
),

parsed as (
    select
        -- Id: account primary key
        "id" AS "Id",

        -- Name: required, fallback to 'Unknown' if empty
        CASE
            WHEN COALESCE(TRIM("name"), '') = '' THEN 'Unknown'
            ELSE TRIM("name")
        END AS "Name",

        -- ERP_Number__c: use the source id as ERP number
        "id" AS "ERP_Number__c",

        -- Customer_Tier__c: map tier to allowed enum values
        CASE UPPER(TRIM(COALESCE("tier", '')))
            WHEN 'GOLD' THEN 'Gold'
            WHEN 'SILVER' THEN 'Silver'
            WHEN 'BRONZE' THEN 'Bronze'
            WHEN 'PLATINUM' THEN 'Platinum'
            ELSE NULL
        END AS "Customer_Tier__c",

        -- Region__c
        TRIM("region") AS "Region__c",

        -- Industry
        INITCAP(TRIM(COALESCE("industry", ''))) AS "Industry",

        -- Website: not in source, default NULL
        NULL AS "Website",

        -- BillingCity: not in source, default NULL
        NULL AS "BillingCity",

        -- BillingCountry: not in source, default NULL
        NULL AS "BillingCountry",

        -- Legacy_Customer_ID__c: from source natural key
        "id" AS "Legacy_Customer_ID__c",

        -- CreatedDate: not in source, default NULL
        NULL AS "CreatedDate",

        -- LastModifiedDate: not in source, default NULL
        NULL AS "LastModifiedDate",

        -- IsDeleted: default 0 (active)
        0 AS "IsDeleted"

    from source
)

select * from parsed
