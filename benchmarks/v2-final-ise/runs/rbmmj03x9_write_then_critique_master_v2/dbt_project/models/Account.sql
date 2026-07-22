{{ config(materialized='table') }}
SELECT
    CONCAT('ACCOUNT-', REPLACE(k."kundennummer", 'CUST-', '')) AS "Id",
    COALESCE(TRIM(k."unternehmensname"), 'Unknown') AS "Name",
    TRIM(k."erp_nr") AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(k."kundenklasse")) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM')
        THEN INITCAP(LOWER(TRIM(k."kundenklasse")))
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(k."vertriebsgebiet") AS "Region__c",
    INITCAP(LOWER(TRIM(k."industrie"))) AS "Industry",
    TRIM(k."homepage") AS "Website",
    TRIM(k."stadt") AS "BillingCity",
    TRIM(k."land_region") AS "BillingCountry",
    TRIM(k."kundennummer") AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }} k