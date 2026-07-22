{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    a.name AS "Name",
    NULL AS "ERP_Number__c",
    a.tier AS "Customer_Tier__c",
    a.region AS "Region__c",
    a.industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    MAX(o.customer_number) AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o ON a.name = o.account_name
GROUP BY a.id, a.name, a.tier, a.region, a.industry