-- This dbt model transforms data from the master_kunden source table into the Account target schema.
{{ config(materialized='table') }}

SELECT
    'ACC-' || m_k.kundennummer AS "Id",
    COALESCE(TRIM(m_k.unternehmensname), 'Unnamed Account ' || m_k.kundennummer) AS "Name",
    TRIM(m_k.erp_nr) AS "ERP_Number__c",
    CASE
        WHEN LOWER(TRIM(m_k.kundenklasse)) = 'gold' THEN 'Gold'
        WHEN LOWER(TRIM(m_k.kundenklasse)) = 'silver' THEN 'Silver'
        WHEN LOWER(TRIM(m_k.kundenklasse)) = 'bronze' THEN 'Bronze'
        WHEN LOWER(TRIM(m_k.kundenklasse)) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(m_k.vertriebsgebiet) AS "Region__c",
    TRIM(m_k.industrie) AS "Industry",
    TRIM(m_k.homepage) AS "Website",
    TRIM(m_k.stadt) AS "BillingCity",
    TRIM(m_k.land_region) AS "BillingCountry",
    m_k.kundennummer AS "Legacy_Customer_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS m_k
