{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    name AS "Name",
    serial_number__c AS "Serial_Number__c",
    CASE
        WHEN warranty_end_date__c IS NULL OR TRIM(warranty_end_date__c) = '' THEN NULL
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' AND warranty_end_date__c != '0000-00-00' THEN TO_DATE(warranty_end_date__c, 'YYYY-MM-DD')::TEXT
        WHEN warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' AND warranty_end_date__c != '0000-00-00' THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')::TEXT
        WHEN warranty_end_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' AND warranty_end_date__c != '0000-00-00' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT
        WHEN warranty_end_date__c ~ '^\d{8}$' AND warranty_end_date__c != '00000000' THEN TO_DATE(warranty_end_date__c, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account__c AS "Account__c",
    project__c AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
