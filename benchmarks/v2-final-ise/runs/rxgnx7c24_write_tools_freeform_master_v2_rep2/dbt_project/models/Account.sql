{{ config(materialized='table') }}

SELECT
    kundennummer AS "Id",
    COALESCE(unternehmensname, 'Unknown') AS "Name",
    erp_nr AS "ERP_Number__c",
    CASE
        WHEN UPPER(kundenklasse) IN ('GOLD', 'SILVER', 'BRONZE', 'PLATINUM') THEN INITCAP(LOWER(kundenklasse))
        ELSE NULL
    END AS "Customer_Tier__c",
    vertriebsgebiet AS "Region__c",
    CASE
        WHEN UPPER(industrie) IN ('MANUFACTURING', 'IT', 'GESUNDHEITSWESEN', 'HEALTHCARE', 'FINANZEN', 'FINANCE', 'TECHNOLOGY', 'INDUSTRIE', 'INDUSTRY') THEN INITCAP(LOWER(industrie))
        ELSE NULL
    END AS "Industry",
    homepage AS "Website",
    stadt AS "BillingCity",
    land_region AS "BillingCountry",
    kundennummer AS "Legacy_Customer_ID__c",
    CURRENT_TIMESTAMP::text AS "CreatedDate",
    CURRENT_TIMESTAMP::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
