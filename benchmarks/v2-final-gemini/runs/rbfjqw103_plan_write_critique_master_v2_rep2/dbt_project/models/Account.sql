{{
    config(materialized='table')
}}

SELECT
    gen_random_uuid()::TEXT AS "Id",
    COALESCE(TRIM(kunde.unternehmensname), 'Unknown Account') AS "Name",
    TRIM(kunde.erp_nr) AS "ERP_Number__c",
    CASE UPPER(TRIM(kunde.kundenklasse))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILBER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATIN' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    TRIM(kunde.vertriebsgebiet) AS "Region__c",
    TRIM(INITCAP(kunde.industrie)) AS "Industry",
    TRIM(LOWER(kunde.homepage)) AS "Website",
    TRIM(INITCAP(kunde.stadt)) AS "BillingCity",
    TRIM(INITCAP(kunde.land_region)) AS "BillingCountry",
    TRIM(kunde.kundennummer) AS "Legacy_Customer_ID__c",
    NOW()::TEXT AS "CreatedDate",
    NOW()::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunde