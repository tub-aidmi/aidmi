{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, id) AS "Name", -- Name is NOT NULL, fallback to id
    serial_number__c AS "Serial_Number__c",
    (CASE
        WHEN TRIM(warranty_end_date__c) IN ('N/A', '0000-00-00', '') THEN NULL
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(warranty_end_date__c, 'YYYY-MM-DD')
        WHEN warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')
        WHEN warranty_end_date__c ~ '^\d{8}$' THEN TO_DATE(warranty_end_date__c, 'YYYYMMDD')
        WHEN warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')
        ELSE NULL
    END)::TEXT AS "Warranty_End_Date__c",
    account__c AS "Account__c",
    project__c AS "Project__c",
    id AS "Legacy_Asset_ID__c", -- Using id as the natural key
    NULL::TEXT AS "CreatedDate", -- Placeholder
    NULL::TEXT AS "LastModifiedDate", -- Placeholder
    0 AS "IsDeleted" -- Default to 0
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
