{{ config(materialized='table') }}

SELECT
    CAST(id AS text) AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    NULL AS "ERP_Number__c",
    CASE LOWER(TRIM(tier))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(region) AS "Region__c",
    TRIM(industry) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
