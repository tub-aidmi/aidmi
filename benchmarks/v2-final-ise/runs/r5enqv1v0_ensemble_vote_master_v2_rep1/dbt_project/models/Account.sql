{{ config(materialized='table') }}

SELECT
    'acc_' || k."kundennummer" AS "Id",
    COALESCE(TRIM(k."unternehmensname"), 'Unknown') AS "Name",
    TRIM(k."erp_nr") AS "ERP_Number__c",
    CASE
        WHEN UPPER(TRIM(k."kundenklasse")) LIKE '%PLATIN%' THEN 'Platinum'
        WHEN UPPER(TRIM(k."kundenklasse")) LIKE '%GOLD%' THEN 'Gold'
        WHEN UPPER(TRIM(k."kundenklasse")) LIKE '%SILBER%' OR UPPER(TRIM(k."kundenklasse")) LIKE '%SILVER%' THEN 'Silver'
        WHEN UPPER(TRIM(k."kundenklasse")) LIKE '%BRONZE%' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(k."vertriebsgebiet") AS "Region__c",
    TRIM(k."industrie") AS "Industry",
    TRIM(k."homepage") AS "Website",
    TRIM(k."stadt") AS "BillingCity",
    TRIM(k."land_region") AS "BillingCountry",
    TRIM(k."kundennummer") AS "Legacy_Customer_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }} k