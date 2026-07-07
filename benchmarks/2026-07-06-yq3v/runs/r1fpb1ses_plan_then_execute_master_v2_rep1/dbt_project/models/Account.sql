-- depends_on: {{ ref("stg_master_kunden") }}

{{ config(materialized='table') }}

SELECT
    MD5(master_kunden.kundennummer)::TEXT AS "Id",
    COALESCE(TRIM(master_kunden.unternehmensname), 'Unknown Account') AS "Name",
    master_kunden.erp_nr AS "ERP_Number__c",
    CASE LOWER(TRIM(master_kunden.kundenklasse))
        WHEN 'gold' THEN 'Gold'
        WHEN 'silver' THEN 'Silver'
        WHEN 'bronze' THEN 'Bronze'
        WHEN 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    master_kunden.vertriebsgebiet AS "Region__c",
    master_kunden.industrie AS "Industry",
    master_kunden.homepage AS "Website",
    master_kunden.stadt AS "BillingCity",
    master_kunden.land_region AS "BillingCountry",
    master_kunden.kundennummer AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS master_kunden