{{ config(materialized='table') }}

SELECT
    -- Salesforce-style Id: prefix with 'C' for cross-table FK consistency
    'C' || TRIM("kundennummer") AS "Id",
    COALESCE(TRIM("unternehmensname"), 'Unknown Customer') AS "Name",
    TRIM("erp_nr") AS "ERP_Number__c",
    -- Map kundenklasse to Customer Tier enum, normalized and validated
    CASE
        WHEN UPPER(TRIM("kundenklasse")) = 'GOLD' THEN 'Gold'
        WHEN UPPER(TRIM("kundenklasse")) = 'SILBER' THEN 'Silver'
        WHEN UPPER(TRIM("kundenklasse")) = 'BRONZE' THEN 'Bronze'
        WHEN UPPER(TRIM("kundenklasse")) = 'PLATIN' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM("vertriebsgebiet") AS "Region__c",
    INITCAP(TRIM("industrie")) AS "Industry",
    TRIM("homepage") AS "Website",
    INITCAP(TRIM("stadt")) AS "BillingCity",
    TRIM("land_region") AS "BillingCountry",
    -- Legacy key from source natural key
    TRIM("kundennummer") AS "Legacy_Customer_ID__c",
    -- Fixed dates since source doesn't have created/modified timestamps
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
