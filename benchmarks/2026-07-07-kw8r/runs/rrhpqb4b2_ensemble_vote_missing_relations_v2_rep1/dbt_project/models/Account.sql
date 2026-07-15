{{ config(materialized='table') }}

WITH account_opportunities AS (
    SELECT 
        a.id AS account_id,
        a.name AS account_name,
        o.customer_number AS legacy_customer_id
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a
    LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o 
        ON a.name = o.account_name
)

SELECT 
    a.id AS "Id",
    a.name AS "Name",
    NULL AS "ERP_Number__c",
    INITCAP(a.tier) AS "Customer_Tier__c",
    a.region AS "Region__c",
    a.industry AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    NULL AS "BillingCountry",
    ao.legacy_customer_id AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a
LEFT JOIN account_opportunities ao ON a.id = ao.account_id