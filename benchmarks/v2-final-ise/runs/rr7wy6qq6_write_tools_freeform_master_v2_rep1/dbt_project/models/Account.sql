{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        kundennummer,
        unternehmensname,
        erp_nr,
        kundenklasse,
        vertriebsgebiet,
        industrie,
        homepage,
        stadt,
        land_region
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

normalized AS (
    SELECT
        kundennummer,
        INITCAP(TRIM(unternehmensname)) AS name,
        TRIM(erp_nr) AS erp_number,
        TRIM(kundenklasse) AS customer_tier,
        TRIM(vertriebsgebiet) AS region,
        TRIM(industrie) AS industry,
        TRIM(homepage) AS website,
        TRIM(stadt) AS billing_city,
        TRIM(land_region) AS billing_country
    FROM source_data
),

account_tier_mapped AS (
    SELECT
        kundennummer,
        name,
        erp_number,
        CASE
            WHEN LOWER(customer_tier) IN ('gold', 'gold customer') THEN 'Gold'
            WHEN LOWER(customer_tier) IN ('silber', 'silver') THEN 'Silver'
            WHEN LOWER(customer_tier) IN ('bronze') THEN 'Bronze'
            WHEN LOWER(customer_tier) IN ('platin', 'platinum') THEN 'Platinum'
            ELSE NULL
        END AS customer_tier__c,
        region,
        industry,
        website,
        billing_city,
        billing_country
    FROM normalized
)

SELECT
    MD5(kundennummer || '_ACCOUNT') AS "Id",
    name AS "Name",
    erp_number AS "ERP_Number__c",
    customer_tier__c AS "Customer_Tier__c",
    region AS "Region__c",
    industry AS "Industry",
    website AS "Website",
    billing_city AS "BillingCity",
    billing_country AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM account_tier_mapped
