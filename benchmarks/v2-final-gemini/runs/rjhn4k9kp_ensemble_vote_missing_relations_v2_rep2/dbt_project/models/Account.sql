{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(a.name, 'Unknown Account') AS "Name",
    NULL::text AS "ERP_Number__c",
    CASE
        WHEN a.tier ILIKE 'Gold' THEN 'Gold'
        WHEN a.tier ILIKE 'Silver' THEN 'Silver'
        WHEN a.tier ILIKE 'Bronze' THEN 'Bronze'
        WHEN a.tier ILIKE 'Platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    a.region AS "Region__c",
    a.industry AS "Industry",
    NULL::text AS "Website",
    NULL::text AS "BillingCity",
    NULL::text AS "BillingCountry",
    a.id AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
