{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), '') AS "Name",
    NULL AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(tier)) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM(tier)) = 'SILVER' THEN 'Silver'
        WHEN UPPER(TRIM(tier)) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM(tier)) = 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    UPPER(TRIM(region)) AS "Region__c",
    UPPER(TRIM(industry)) AS "Industry",
    NULL AS "Website",
    NULL AS "BillingCity",
    CASE
        WHEN UPPER(TRIM(region)) = 'DACH' THEN 'DE'
        WHEN UPPER(TRIM(region)) = 'BENELUX' THEN 'NL'
        WHEN UPPER(TRIM(region)) = 'NORDICS' THEN 'SE'
        WHEN UPPER(TRIM(region)) = 'UK' THEN 'GB'
        WHEN UPPER(TRIM(region)) LIKE '%SOUTHERN%' THEN 'IT'
        ELSE NULL
    END AS "BillingCountry",
    id AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
