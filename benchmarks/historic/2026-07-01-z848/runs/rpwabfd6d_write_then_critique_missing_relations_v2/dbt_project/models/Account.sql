-- models/Account.sql

{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(a.name, 'Unknown Account') AS "Name",
    o.customer_number AS "ERP_Number__c",
    CASE
        WHEN LOWER(a.tier) = 'gold' THEN 'Gold'
        WHEN LOWER(a.tier) = 'silver' THEN 'Silver'
        WHEN LOWER(a.tier) = 'bronze' THEN 'Bronze'
        WHEN LOWER(a.tier) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    a.region AS "Region__c",
    a.industry AS "Industry",
    CAST(NULL AS TEXT) AS "Website",
    CAST(NULL AS TEXT) AS "BillingCity",
    CAST(NULL AS TEXT) AS "BillingCountry",
    CAST(NULL AS TEXT) AS "Legacy_Customer_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
LEFT JOIN (
    SELECT
        opp.account_name AS account_name,
        MIN(opp.customer_number) AS customer_number
    FROM
        {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS opp
    WHERE
        opp.account_name IS NOT NULL
    GROUP BY
        opp.account_name
) AS o
ON a.name = o.account_name