
{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown') AS "Name",
    CAST(NULL AS TEXT) AS "ERP_Number__c",
    CASE
        WHEN LOWER(tier) = 'gold' THEN 'Gold'
        WHEN LOWER(tier) = 'silver' THEN 'Silver'
        WHEN LOWER(tier) = 'bronze' THEN 'Bronze'
        WHEN LOWER(tier) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    CAST(NULL AS TEXT) AS "Website",
    CAST(NULL AS TEXT) AS "BillingCity",
    CAST(NULL AS TEXT) AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_src', 'Account') }}
