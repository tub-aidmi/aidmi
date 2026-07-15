{{ config(materialized='table') }}

with src as (
    select * from {{ source('fixture_master_v2_src', 'master_kunden') }}
)

select
    -- Salesforce-style ID derived from source natural key
    SUBSTRING(MD5(kundennummer), 1, 18) AS "Id",
    -- Name
    INITCAP(TRIM(unternehmensname)) AS "Name",
    -- ERP Number
    CAST(erp_nr AS text) AS "ERP_Number__c",
    -- Customer Tier: map kundenklasse to Gold/Silver/Bronze/Platinum
    CASE LOWER(TRIM(kundenklasse))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        WHEN 'platin' THEN 'Platinum'
        WHEN 'premium' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    -- Region
    INITCAP(TRIM(vertriebsgebiet)) AS "Region__c",
    -- Industry
    INITCAP(TRIM(industrie)) AS "Industry",
    -- Website
    homepage AS "Website",
    -- Billing City
    INITCAP(TRIM(stadt)) AS "BillingCity",
    -- Billing Country
    INITCAP(TRIM(land_region)) AS "BillingCountry",
    -- Legacy ID
    kundennummer AS "Legacy_Customer_ID__c",
    -- CreatedDate - use a default since source doesn't have timestamps
    CURRENT_DATE::text AS "CreatedDate",
    -- LastModifiedDate
    CURRENT_DATE::text AS "LastModifiedDate",
    -- IsDeleted
    0 AS "IsDeleted"
from src
