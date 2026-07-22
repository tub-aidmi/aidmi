{{ config(materialized='table') }}

SELECT
    -- Salesforce-style Account Id: prefix kunden_nr with '001' (standard SFDC account prefix)
    '001' || TRIM(kunden_nr) AS "Id",

    -- Name maps to firmenname; coalesce for safety
    COALESCE(firmenname, '') AS "Name",

    -- ERP_Number__c
    TRIM(erp_nummer) AS "ERP_Number__c",

    -- Customer_Tier__c: map kategorie values to (Gold, Silver, Bronze, Platinum)
    CASE UPPER(TRIM(kategorie))
        WHEN 'PLATINUM' THEN 'Platinum'
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        -- Handle common alternate spellings / translations
        WHEN 'PREMIUM' THEN 'Gold'
        WHEN 'STANDARD' THEN 'Silver'
        WHEN 'BASE' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",

    -- Region__c
    TRIM(gebiet) AS "Region__c",

    -- Industry (branche)
    INITCAP(TRIM(branche)) AS "Industry",

    -- Website
    LOWER(TRIM(webseite)) AS "Website",

    -- BillingCity
    INITCAP(TRIM(ort)) AS "BillingCity",

    -- BillingCountry (assumed to be 2-letter ISO or full name)
    UPPER(TRIM(land)) AS "BillingCountry",

    -- Legacy natural key preserved for row-level verification
    TRIM(kunden_nr) AS "Legacy_Customer_ID__c",

    -- Not present in source; default NULL/0
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}