-- models/Account.sql

{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Account') AS "Name", -- Name is NOT NULL
    NULL AS "ERP_Number__c",
    CASE tier
        WHEN 'Gold' THEN 'Gold'
        WHEN 'Silver' THEN 'Silver'
        WHEN 'Bronze' THEN 'Bronze'
        WHEN 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }}
