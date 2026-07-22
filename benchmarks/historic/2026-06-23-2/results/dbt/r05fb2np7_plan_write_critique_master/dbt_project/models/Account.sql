{{ config(materialized='table') }}

SELECT
    k.kundennummer AS Id,
    COALESCE(TRIM(k.unternehmensname), '') AS Name,
    k.erp_nr AS ERP_Number__c,
    CASE LOWER(TRIM(k.kundenklasse))
        WHEN 'gold'          THEN 'Gold'
        WHEN 'silber'        THEN 'Silver'
        WHEN 'silver'        THEN 'Silver'
        WHEN 'bronze'        THEN 'Bronze'
        WHEN 'platin'        THEN 'Platinum'
        WHEN 'platinum'      THEN 'Platinum'
        ELSE NULL
    END AS Customer_Tier__c,
    k.vertriebsgebiet AS Region__c,
    k.industrie AS Industry,
    k.homepage AS Website,
    k.stadt AS BillingCity,
    k.land_region AS BillingCountry,
    k.kundennummer AS Legacy_Customer_ID__c,
    NULL::text AS CreatedDate,
    NULL::text AS LastModifiedDate,
    0::integer AS IsDeleted

FROM {{ source('fixture_master_src', 'master_kunden') }} k