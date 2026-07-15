{{ config(materialized='table') }}

SELECT
    -- Generate a Salesforce-style Id (18-character lowercase alphanumeric)
    LOWER(
        SUBSTRING(
            ENCODE(DIGEST(k."kundennummer", 'sha256'), 'hex'),
            1, 18
        )
    ) AS "Id",
    
    -- Map source fields to target columns
    k."unternehmensname" AS "Name",
    k."erp_nr" AS "ERP_Number__c",
    
    -- Map kundenklasse to Customer_Tier__c enum
    CASE 
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('PLATINUM', 'PLATIN') THEN 'Platinum'
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('GOLD', 'GOLDEN') THEN 'Gold'
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('SILBER', 'SILVER') THEN 'Silver'
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('BRONZE', 'BRONZE') THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    
    -- Map vertriebsgebiet to Region__c
    INITCAP(TRIM(k."vertriebsgebiet")) AS "Region__c",
    
    -- Map industrie to Industry
    INITCAP(TRIM(k."industrie")) AS "Industry",
    
    -- Map homepage to Website
    TRIM(k."homepage") AS "Website",
    
    -- Map stadt to BillingCity
    INITCAP(TRIM(k."stadt")) AS "BillingCity",
    
    -- Map land_region to BillingCountry
    UPPER(TRIM(k."land_region")) AS "BillingCountry",
    
    -- Legacy customer ID
    k."kundennummer" AS "Legacy_Customer_ID__c",
    
    -- Default dates and flags
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }} k