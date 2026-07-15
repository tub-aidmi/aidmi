{{ config(materialized='table') }}

WITH normalized AS (
    SELECT
        -- Key transform: strip leading non-alphanumeric prefix, trim, uppercase
        UPPER(TRIM(REGEXP_REPLACE(id, '^\D+', '', 'g'))) AS Id,
        "name" AS "Name",
        -- ERP_Number__c not available from source
        NULL AS "ERP_Number__c",
        CASE LOWER(TRIM(tier))
            WHEN 'gold' THEN 'Gold'
            WHEN 'silver' THEN 'Silver'
            WHEN 'bronze' THEN 'Bronze'
            WHEN 'platinum' THEN 'Platinum'
            ELSE NULL
        END AS "Customer_Tier__c",
        TRIM(region) AS "Region__c",
        INITCAP(TRIM(industry)) AS "Industry",
        -- Not available from source
        NULL AS "Website",
        NULL AS "BillingCity",
        NULL AS "BillingCountry",
        id AS "Legacy_Customer_ID__c",
        NULL AS "CreatedDate",
        NULL AS "LastModifiedDate",
        0 AS "IsDeleted"
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)

SELECT * FROM normalized
