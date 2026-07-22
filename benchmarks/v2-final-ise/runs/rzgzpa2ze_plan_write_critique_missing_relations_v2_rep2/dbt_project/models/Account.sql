{{ config(materialized='table') }}

SELECT
    TRIM(UPPER(id)) AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unknown Account') AS "Name",
    TRIM(id) AS "ERP_Number__c",
    CASE
        WHEN INITCAP(TRIM(tier)) IN ('Gold', 'Silver', 'Bronze', 'Platinum')
            THEN INITCAP(TRIM(tier))
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(region) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    NULL::TEXT AS "Website",
    NULL::TEXT AS "BillingCity",
    NULL::TEXT AS "BillingCountry",
    TRIM(id) AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}