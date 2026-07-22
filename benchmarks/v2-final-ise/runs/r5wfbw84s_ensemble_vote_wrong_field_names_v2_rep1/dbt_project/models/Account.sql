{{ config(materialized='table') }}

SELECT
    '001' || RIGHT('00000000' || kunden_nr, 8) AS "Id",
    COALESCE(TRIM(firmenname), 'Unnamed Account') AS "Name",
    erp_nummer AS "ERP_Number__c",
    CASE UPPER(TRIM(kategorie))
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'SILBER' THEN 'Silver'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'PLATIN' THEN 'Platinum'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",
    gebiet AS "Region__c",
    branche AS "Industry",
    webseite AS "Website",
    ort AS "BillingCity",
    land AS "BillingCountry",
    kunden_nr AS "Legacy_Customer_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}