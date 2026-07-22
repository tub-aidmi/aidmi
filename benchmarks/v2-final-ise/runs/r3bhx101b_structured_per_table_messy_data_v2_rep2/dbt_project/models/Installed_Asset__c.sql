{{ config(materialized='table') }}

SELECT 
    i.id AS "Id",
    COALESCE(NULLIF(TRIM(i.name), ''), 'Unnamed Asset') AS "Name",
    TRIM(i.serial_number__c) AS "Serial_Number__c",
    CASE 
        WHEN i.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN i.warranty_end_date__c
        WHEN i.warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(i.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN i.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(i.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN i.warranty_end_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(i.warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    a.id AS "Account__c",
    p.id AS "Project__c",
    TRIM(i.serial_number__c) AS "Legacy_Asset_ID__c",
    CURRENT_DATE AS "CreatedDate",
    CURRENT_DATE AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} i
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON i.account__c = a.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'project__c') }} p ON i.project__c = p.id