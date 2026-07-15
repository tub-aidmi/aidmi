{{ config(materialized='table') }}

SELECT
    ia.id AS "Id",
    ia.name AS "Name",
    ia.serial_number__c AS "Serial_Number__c",
    CASE
        WHEN ia.warranty_end_date__c IS NULL THEN NULL
        WHEN ia.warranty_end_date__c = '0000-00-00' THEN NULL
        WHEN ia.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(ia.warranty_end_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN ia.warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(ia.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN ia.warranty_end_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ia.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ia.warranty_end_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ia.warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    a.id AS "Account__c",
    ia.project__c AS "Project__c",
    ia.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} ia
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON ia.account__c = a.id
