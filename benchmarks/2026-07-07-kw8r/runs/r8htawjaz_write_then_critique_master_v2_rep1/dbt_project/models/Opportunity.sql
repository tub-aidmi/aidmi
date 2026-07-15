{{ config(materialized='table') }}

SELECT
    '001' || REGEXP_REPLACE(kundennummer, '[^a-z0-9]', '', 'i') AS "Id",
    COALESCE(unternehmensname, 'Unnamed Account') AS "Name",
    erp_nr AS "ERP_Number__c",
    INITCAP(LOWER(TRIM(kundenklasse))) AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    industrie AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_kunden') }}