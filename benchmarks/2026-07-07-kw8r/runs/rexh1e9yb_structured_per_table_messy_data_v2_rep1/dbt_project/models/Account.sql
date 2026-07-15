{{ config(materialized='table') }}

WITH source AS (
    SELECT *
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
),

transformed AS (
    SELECT
        -- Id: source natural key, no transformation needed
        CAST(id AS TEXT) AS "Id",

        -- Name: NOT NULL constraint — replace nulls with placeholder
        COALESCE(
            INITCAP(TRIM(name)),
            'Unknown Account'
        ) AS "Name",

        -- ERP_Number__c: map from erp_number__c
        CAST(erp_number__c AS TEXT) AS "ERP_Number__c",

        -- Customer_Tier__c: normalize case + translate German values to English enum
        CASE LOWER(TRIM(customer_tier__c))
            WHEN 'bronze'   THEN 'Bronze'
            WHEN 'silver'   THEN 'Silver'
            WHEN 'silber'   THEN 'Silver'  -- DE translation
            WHEN 'gold'     THEN 'Gold'
            WHEN 'platinum' THEN 'Platinum'
            WHEN 'platin'   THEN 'Platinum'  -- DE translation
            ELSE NULL
        END AS "Customer_Tier__c",

        -- Region__c: clean up whitespace, initcap (values already consistent)
        INITCAP(TRIM(region__c)) AS "Region__c",

        -- Industry: translate German values to English where they're duplicates of existing English terms
        CASE LOWER(TRIM(industry))
            WHEN 'finance'    THEN 'Finance'
            WHEN 'finanzen'   THEN 'Finance'  -- DE translation
            WHEN 'healthcare' THEN 'Healthcare'
            WHEN 'gesundheitswesen' THEN 'Healthcare'  -- DE translation
            WHEN 'it'         THEN 'IT'
            WHEN 'manufacturing' THEN 'Manufacturing'
            WHEN 'industrie'    THEN 'Manufacturing'  -- DE translation
            WHEN 'technology' THEN 'Technology'
            WHEN 'technologie'  THEN 'Technology'  -- DE translation
            ELSE INITCAP(TRIM(industry))
        END AS "Industry",

        -- Website: preserve as-is, trim whitespace
        TRIM(website) AS "Website",

        -- BillingCity: initcap + trim
        INITCAP(TRIM(billingcity)) AS "BillingCity",

        -- BillingCountry: initcap + trim
        INITCAP(TRIM(billingcountry)) AS "BillingCountry",

        -- Legacy_Customer_ID__c: store the original source key for auditability
        CAST(id AS TEXT) AS "Legacy_Customer_ID__c",

        -- Audit columns not in source — provide stable defaults
        NULL::TEXT AS "CreatedDate",
        NULL::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"

    FROM source
)

SELECT * FROM transformed