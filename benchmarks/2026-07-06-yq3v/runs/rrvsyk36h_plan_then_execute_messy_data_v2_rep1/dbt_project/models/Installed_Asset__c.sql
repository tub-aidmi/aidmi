{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown Asset') AS "Name",
    TRIM(serial_number__c) AS "Serial_Number__c",
    CASE
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(warranty_end_date__c, 'YYYY-MM-DD')::TEXT
        WHEN warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT
        WHEN warranty_end_date__c ~ '^\d{8}$' THEN TO_DATE(warranty_end_date__c, 'YYYYMMDD')::TEXT
        WHEN warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account__c AS "Account__c",
    project__c AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
