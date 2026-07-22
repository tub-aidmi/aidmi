{{
    config(materialized='table')
}}

SELECT
    MD5(TRIM(kundennummer)) AS "Id",
    COALESCE(TRIM(unternehmensname), 'Unknown Account') AS "Name",
    TRIM(erp_nr) AS "ERP_Number__c",
    CASE TRIM(kundenklasse)
        WHEN 'Goldkunde' THEN 'Gold'
        WHEN 'Silberkunde' THEN 'Silver'
        WHEN 'Bronzekunde' THEN 'Bronze'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(vertriebsgebiet) AS "Region__c",
    TRIM(industrie) AS "Industry",
    TRIM(homepage) AS "Website",
    TRIM(stadt) AS "BillingCity",
    TRIM(land_region) AS "BillingCountry",
    TRIM(kundennummer) AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }}