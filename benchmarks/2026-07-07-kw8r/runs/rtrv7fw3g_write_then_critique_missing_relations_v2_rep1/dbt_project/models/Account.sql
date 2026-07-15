{{ config(materialized='table') }}
SELECT
    a."id" AS "Id",
    COALESCE(NULLIF(TRIM(a."name"), ''), 'Unknown') AS "Name",
    MIN(o."customer_number") AS "ERP_Number__c",
    CASE WHEN TRIM(a."tier") IN ('Gold', 'Silver', 'Bronze', 'Platinum') THEN TRIM(a."tier") ELSE NULL END AS "Customer_Tier__c",
    TRIM(a."region") AS "Region__c",
    TRIM(a."industry") AS "Industry",
    NULL::text AS "Website",
    NULL::text AS "BillingCity",
    NULL::text AS "BillingCountry",
    a."id" AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'account') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'opportunity') }} o 
    ON TRIM(a."name") = TRIM(o."account_name")
GROUP BY a."id", a."name", a."tier", a."region", a."industry"