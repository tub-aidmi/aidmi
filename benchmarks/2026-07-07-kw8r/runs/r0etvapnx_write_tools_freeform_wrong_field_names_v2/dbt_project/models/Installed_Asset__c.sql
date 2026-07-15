{{ config(materialized='table') }}

SELECT 
    '001' || RIGHT('000000' || REGEXP_REPLACE(a.asset_id, '[^0-9]', '', 'g'), 6) AS "Id",
    a.bezeichnung AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE 
        WHEN a.garantie_bis IS NOT NULL AND a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantie_bis
        WHEN a.garantie_bis IS NOT NULL AND a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantie_bis IS NOT NULL AND a.garantie_bis ~ '^\d{8}$' THEN SUBSTR(a.garantie_bis, 1, 4) || '-' || SUBSTR(a.garantie_bis, 5, 2) || '-' || SUBSTR(a.garantie_bis, 7, 2)
        ELSE NULL
    END AS "Warranty_End_Date__c",
    '001' || RIGHT('000000' || REGEXP_REPLACE(a.kd_ref, '[^0-9]', '', 'g'), 6) AS "Account__c",
    CASE 
        WHEN a.projekt_ref IS NOT NULL THEN '001' || RIGHT('000000' || REGEXP_REPLACE(a.projekt_ref, '[^0-9]', '', 'g'), 6)
        ELSE NULL
    END AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
