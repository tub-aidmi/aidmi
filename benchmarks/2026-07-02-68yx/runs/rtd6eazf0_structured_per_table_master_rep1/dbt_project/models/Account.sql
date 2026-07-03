{{ config(materialized='table') }}

SELECT
    -- Generate a deterministic 18-character Salesforce-like ID from the legacy key
    SUBSTR(MD5('Account:' || kundennummer), 1, 18) AS "Id",
    -- Company name; fallback for NULL/empty
    COALESCE(NULLIF(TRIM(unternehmensname), ''), 'Unknown Customer') AS "Name",
    -- ERP number as-is from source (includes ERP- prefix)
    erp_nr AS "ERP_Number__c",
    -- Normalize customer tier to standard enum values
    CASE
        WHEN LOWER(kundenklasse) IN ('gold') THEN 'Gold'
        WHEN LOWER(kundenklasse) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(kundenklasse) IN ('bronze') THEN 'Bronze'
        WHEN LOWER(kundenklasse) IN ('platinum', 'platin') THEN 'Platinum'
    END AS "Customer_Tier__c",
    -- Sales region; trim whitespace, convert empty strings to NULL
    NULLIF(TRIM(vertriebsgebiet), '') AS "Region__c",
    -- Industry; apply INITCAP for consistent casing
    INITCAP(industrie) AS "Industry",
    -- Website/homepage as-is
    homepage AS "Website",
    -- City with proper casing
    INITCAP(stadt) AS "BillingCity",
    -- Country with proper casing
    INITCAP(land_region) AS "BillingCountry",
    -- Legacy source key preserved
    kundennummer AS "Legacy_Customer_ID__c",
    -- Default audit fields (initial load, no native timestamps in source)
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_kunden') }}