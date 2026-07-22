{{ config(materialized='table') }}

SELECT 
    -- Generate Salesforce-style 18-character Account ID from natural key kundennummer
    CASE 
        WHEN TRIM(kundennummer) IS NOT NULL AND TRIM(kundennummer) != ''
        THEN '001' || LPAD(TRIM(kundennummer), 15, '0')
        ELSE '001XXXXXXXXXXX000'
    END AS "Id",

    -- Company name derived from unternehmensname; fallback for blanks
    CASE 
        WHEN TRIM(unternehmensname) IS NOT NULL AND TRIM(unternehmensname) != ''
        THEN INITCAP(TRIM(unternehmensname))
        ELSE 'Unknown Customer'
    END AS "Name",

    -- ERP number (native text field preserved as-is when present)
    CASE 
        WHEN TRIM(erp_nr) IS NOT NULL AND TRIM(erp_nr) != ''
        THEN TRIM(erp_nr)
        ELSE NULL
    END AS "ERP_Number__c",

    -- Customer tier mapped from kundenklasse into the target enum domain
    CASE UPPER(TRIM(kundenklasse))
        WHEN 'GOLD'       THEN 'Gold'
        WHEN 'PLATINUM'   THEN 'Platinum'
        WHEN 'SILVER'     THEN 'Silver'
        WHEN 'BRONZE'     THEN 'Bronze'
        WHEN '1'          THEN 'Gold'
        WHEN '2'          THEN 'Silver'
        WHEN '3'          THEN 'Bronze'
        WHEN '4'          THEN 'Platinum'
        WHEN 'PREMIUM'    THEN 'Gold'
        WHEN 'STANDARD'   THEN 'Silver'
        ELSE NULL
    END AS "Customer_Tier__c",

    -- Sales region / territory
    CASE 
        WHEN TRIM(vertriebsgebiet) IS NOT NULL AND TRIM(vertriebsgebiet) != ''
        THEN INITCAP(TRIM(vertriebsgebiet))
        ELSE NULL
    END AS "Region__c",

    -- Industry classification
    CASE 
        WHEN TRIM(industrie) IS NOT NULL AND TRIM(industrie) != ''
        THEN INITCAP(TRIM(industrie))
        ELSE NULL
    END AS "Industry",

    -- Corporate website / homepage
    CASE 
        WHEN TRIM(homepage) IS NOT NULL AND TRIM(homepage) != ''
        THEN TRIM(homepage)
        ELSE NULL
    END AS "Website",

    -- Billing city (Stadt → Account.BillingCity)
    CASE 
        WHEN TRIM(stadt) IS NOT NULL AND TRIM(stadt) != ''
        THEN INITCAP(TRIM(stadt))
        ELSE NULL
    END AS "BillingCity",

    -- Billing country or region code
    CASE 
        WHEN TRIM(land_region) IS NOT NULL AND TRIM(land_region) != ''
        THEN INITCAP(TRIM(land_region))
        ELSE NULL
    END AS "BillingCountry",

    -- Natural key preserved for row-level reconciliation
    kundennummer AS "Legacy_Customer_ID__c",

    -- CreatedDate not available in source; left NULL
    NULL AS "CreatedDate",

    -- LastModifiedDate not available in source; left NULL
    NULL AS "LastModifiedDate",

    -- Deletion flag — no historical delete records in the source
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
WHERE kundennummer IS NOT NULL