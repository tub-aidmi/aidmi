{{ config(materialized='table') }}

SELECT
    -- Salesforce-style Id: extract numeric part from kundennummer, zero-pad to 6 digits, prefix with Account type '001'
    '001' || LPAD(regexp_replace(kundennummer, '[^0-9]', '', 'g'), 6, '0') AS "Id",

    -- Name: NOT NULL — use company name or fallback
    COALESCE(INITCAP(unternehmensname), 'Unknown Customer - ' || kundennummer) AS "Name",

    -- ERP Number
    TRIM(erp_nr) AS "ERP_Number__c",

    -- Customer Tier: map German/English variants to standard enum values
    CASE LOWER(TRIM(kundenklasse))
        WHEN 'gold'       THEN 'Gold'
        WHEN 'silver'     THEN 'Silver'
        WHEN 'bronze'     THEN 'Bronze'
        WHEN 'platin'     THEN 'Platinum'
        WHEN 'platinum'   THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",

    -- Region
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",

    -- Industry: normalise to Title Case
    INITCAP(TRIM(industrie)) AS "Industry",

    -- Website: ensure consistent URL format (prepend https:// if missing http)
    CASE
        WHEN homepage IS NULL THEN NULL
        WHEN homepage ~ '^https?://' THEN TRIM(homepage)
        ELSE 'https://' || TRIM(homepage)
    END AS "Website",

    -- Billing City
    INITCAP(TRIM(stadt)) AS "BillingCity",

    -- Billing Country
    INITCAP(TRIM(land_region)) AS "BillingCountry",

    -- Legacy key for row-level verification
    kundennummer AS "Legacy_Customer_ID__c",

    -- No source date columns — use a stable default ISO datetime string
    '2024-01-01 00:00:00' AS "CreatedDate",
    '2024-01-01 00:00:00' AS "LastModifiedDate",

    -- Not deleted by default
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }}