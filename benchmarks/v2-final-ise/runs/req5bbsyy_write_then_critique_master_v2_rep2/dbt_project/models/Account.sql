{{ config(materialized='table') }}

SELECT 
    -- Salesforce-style Account ID: extract digits from kundennummer, zero-pad to 9 digits, prefix with '001'
    -- Regex guards against non-numeric suffixes (e.g. CUST-M1001) that would break SUBSTRING+CAST
    '001' || LPAD(REGEXP_REPLACE(kundennummer, '[^0-9]', ''), 9, '0') AS "Id",

    -- Company name with fallback for nulls; use REGEXP_REPLACE to strip 'CUST-' prefix safely
    COALESCE(
        unternehmensname,
        'Kunde ' || REGEXP_REPLACE(kundennummer, '^CUST-', '')
    ) AS "Name",

    -- ERP number (keep as-is from source)
    CAST(erp_nr AS TEXT) AS "ERP_Number__c",

    -- Customer tier mapping: normalize German/English variants and mixed case to standard enum values
    CASE 
        WHEN LOWER(TRIM(COALESCE(kundenklasse, ''))) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(COALESCE(kundenklasse, ''))) IN ('silver', 'silber') THEN 'Silver'
        WHEN LOWER(TRIM(COALESCE(kundenklasse, ''))) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(COALESCE(kundenklasse, ''))) IN ('platin', 'platinum') THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",

    -- Sales region (vertriebsgebiet) — keep as-is with initial capitalization
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",

    -- Industry normalization: German → English mappings with fallback to source value normalized
    CASE 
        WHEN LOWER(TRIM(COALESCE(industrie, ''))) = 'finanz' OR LOWER(TRIM(COALESCE(industrie, ''))) = 'finanzen' THEN 'Finance'
        WHEN LOWER(TRIM(COALESCE(industrie, ''))) IN ('healthcare', 'gesundheitswesen') THEN 'Healthcare'
        WHEN LOWER(TRIM(COALESCE(industrie, ''))) IN ('it', 'technologie', 'technik') THEN 'Technology'
        WHEN LOWER(TRIM(COALESCE(industrie, ''))) = 'industrie' THEN 'Industrial'
        WHEN LOWER(TRIM(COALESCE(industrie, ''))) = 'manufacturing' THEN 'Manufacturing'
        ELSE INITCAP(TRIM(COALESCE(industrie, '')))
    END AS "Industry",

    -- Website/homepage URL
    homepage AS "Website",

    -- Billing city (INITCAP for consistent casing)
    INITCAP(TRIM(stadt)) AS "BillingCity",

    -- Billing country (keep as-is from source)
    land_region AS "BillingCountry",

    -- Legacy customer ID (natural key for row-level verification)
    kundennummer AS "Legacy_Customer_ID__c",

    -- No temporal data in master_kunden; use NULL per guideline
    NULL::TEXT AS "CreatedDate",

    -- No temporal data in master_kunden; use NULL per guideline
    NULL::TEXT AS "LastModifiedDate",

    -- Not deleted by default
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }}