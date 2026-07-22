{{ config(materialized='table') }}

WITH cleaned AS (
    SELECT
        -- Salesforce-style Account Id: prefix with "001" to match SFDC format
        '001' || REGEXP_REPLACE(TRIM(id), '[^0-9]', '', 'g') AS "Id",

        -- Name: normalized title case, default for empty/null
        COALESCE(NULLIF(TRIM(name), ''), 'Unknown Account') AS "Name",

        -- ERP Number: clean whitespace
        TRIM(erp_number__c) AS "ERP_Number__c",

        -- Customer Tier: enum mapping (Gold, Silver, Bronze, Platinum)
        CASE
            WHEN LOWER(TRIM(customer_tier__c)) IN ('gold', 'platinum', 'silver', 'bronze') THEN INITCAP(TRIM(customer_tier__c))
            ELSE NULL
        END AS "Customer_Tier__c",

        -- Region: uppercase normalization
        UPPER(TRIM(region__c)) AS "Region__c",

        -- Industry: title case
        INITCAP(TRIM(industry)) AS "Industry",

        -- Website: lowercase, ensure http prefix absent (clean)
        LOWER(TRIM(website)) AS "Website",

        -- Billing City: title case
        INITCAP(TRIM(billingcity)) AS "BillingCity",

        -- Billing Country: uppercase for ISO codes or full names
        UPPER(TRIM(billingcountry)) AS "BillingCountry",

        -- Legacy Customer ID from source natural key
        TRIM(id) AS "Legacy_Customer_ID__c"

    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
    WHERE TRIM(id) IS NOT NULL AND TRIM(id) != ''
)

SELECT
    "Id",
    "Name",
    "ERP_Number__c",
    "Customer_Tier__c",
    "Region__c",
    "Industry",
    "Website",
    "BillingCity",
    "BillingCountry",
    "Legacy_Customer_ID__c",

    -- Derived date fields (not in source): use placeholder ISO dates
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",

    -- IsDeleted: not in source, default to 0 (false)
    0 AS "IsDeleted"

FROM cleaned