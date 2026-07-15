{{ config(materialized='table') }}
SELECT 
    a.id AS "Id",
    COALESCE(NULLIF(TRIM(a.name), ''), 'Unknown') AS "Name",
    NULL::text AS "ERP_Number__c",
    CASE 
        WHEN LOWER(TRIM(a.tier)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(a.tier)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(a.tier)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(a.tier)) = 'platinum' THEN 'Platinum'
        ELSE NULL 
    END AS "Customer_Tier__c",
    a.region AS "Region__c",
    a.industry AS "Industry",
    NULL::text AS "Website",
    NULL::text AS "BillingCity",
    NULL::text AS "BillingCountry",
    (
        SELECT o.customer_number
        FROM {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o
        WHERE LOWER(TRIM(o.account_name)) = LOWER(TRIM(a.name))
        ORDER BY o.id
        LIMIT 1
    ) AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a