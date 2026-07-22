{{
    config(materialized='table')
}}

SELECT
    MD5(mk.kundennummer) AS "Id",
    COALESCE(mk.unternehmensname, 'Unknown Account ' || mk.kundennummer) AS "Name",
    mk.erp_nr AS "ERP_Number__c",
    CASE
        WHEN LOWER(mk.kundenklasse) = 'gold' THEN 'Gold'
        WHEN LOWER(mk.kundenklasse) = 'silver' THEN 'Silver'
        WHEN LOWER(mk.kundenklasse) = 'bronze' THEN 'Bronze'
        WHEN LOWER(mk.kundenklasse) = 'platinum' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    mk.vertriebsgebiet AS "Region__c",
    mk.industrie AS "Industry",
    mk.homepage AS "Website",
    mk.stadt AS "BillingCity",
    mk.land_region AS "BillingCountry",
    mk.kundennummer AS "Legacy_Customer_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk