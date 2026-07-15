{{ config(materialized='table') }}

SELECT 
    -- Salesforce-style Account Id: deterministic, 18-char ID prefixed with "001"
    '001' || LEFT(MD5('001' || id::text), 13) AS "Id",
    
    -- Name must be NOT NULL; default to 'Unknown' if missing or blank
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    
    -- ERP_Number__c: no explicit source column available
    NULL AS "ERP_Number__c",
    
    -- Customer_Tier__c: map from source `tier` with case-insensitive enum normalization
    CASE LOWER(TRIM(tier))
        WHEN 'gold'      THEN 'Gold'
        WHEN 'silver'    THEN 'Silver'
        WHEN 'bronze'    THEN 'Bronze'
        WHEN 'platinum'  THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    
    -- Region__c: trimmed, title-cased for consistency
    INITCAP(TRIM(region)) AS "Region__c",
    
    -- Industry: trimmed verbatim from source
    TRIM(industry) AS "Industry",
    
    -- Website: not present in source; leave NULL
    NULL AS "Website",
    
    -- BillingCity: not present in source; leave NULL
    NULL AS "BillingCity",
    
    -- BillingCountry: not present in source; leave NULL
    NULL AS "BillingCountry",
    
    -- Legacy_Customer_ID__c: the original source natural key for traceability
    TRIM(id) AS "Legacy_Customer_ID__c",
    
    -- CreatedDate / LastModifiedDate: no timestamps in raw source data
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    
    -- IsDeleted: 0 = not deleted (default state)
    0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
WHERE id IS NOT NULL