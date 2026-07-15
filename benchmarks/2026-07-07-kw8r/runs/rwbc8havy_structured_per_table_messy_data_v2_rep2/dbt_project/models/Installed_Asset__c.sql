{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    CAST(name AS TEXT) AS "Name",
    CAST(serial_number__c AS TEXT) AS "Serial_Number__c",
    CASE
        WHEN warranty_end_date__c IS NULL OR warranty_end_date__c = '0000-00-00' OR warranty_end_date__c = 'N/A' THEN NULL
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN warranty_end_date__c
        WHEN warranty_end_date__c ~ '^\d{8}$' THEN TO_DATE(warranty_end_date__c, 'YYYYMMDD')::TEXT
        WHEN warranty_end_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT
        WHEN warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CAST(account__c AS TEXT) AS "Account__c",
    CAST(project__c AS TEXT) AS "Project__c",
    CAST(id AS TEXT) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}