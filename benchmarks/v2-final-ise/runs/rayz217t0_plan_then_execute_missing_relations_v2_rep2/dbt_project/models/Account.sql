{{ config(materialized='table') }}

SELECT
    TRIM(UPPER(id)) AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    id AS "ERP_Number__c",
    CASE 
        WHEN INITCAP(TRIM(tier)) IN ('Gold', 'Silver', 'Bronze', 'Platinum') THEN INITCAP(TRIM(tier))
        ELSE NULL 
    END AS "Customer_Tier__c",
    INITCAP(TRIM(region)) AS "Region__c",
    INITCAP(TRIM(industry)) AS "Industry",
    'N/A' AS "Website",
    NULL::text AS "BillingCity",
    NULL::text AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    CAST(NOW() AS TEXT) AS "CreatedDate",
    CAST(NOW() AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}