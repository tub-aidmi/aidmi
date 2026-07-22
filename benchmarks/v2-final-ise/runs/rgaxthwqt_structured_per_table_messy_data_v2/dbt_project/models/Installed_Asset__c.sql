{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), '') AS "Name",
    CAST(serial_number__c AS TEXT) AS "Serial_Number__c",
    CASE
        WHEN warranty_end_date__c IS NULL OR TRIM(warranty_end_date__c) = '' THEN NULL
        -- Handle DD.MM.YYYY format (common in German/European data)
        WHEN warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'DD.MM.YYYY')::TEXT
        -- Handle YYYY-MM-DD format (ISO standard)
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'YYYY-MM-DD')::TEXT
        -- Handle MM/DD/YYYY format (US style)
        WHEN warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'MM/DD/YYYY')::TEXT
        -- Handle DD/MM/YYYY format
        WHEN warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'DD/MM/YYYY')::TEXT
        -- Handle YYYYMMDD format
        WHEN warranty_end_date__c ~ '^\d{8}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CAST(account__c AS TEXT) AS "Account__c",
    CAST(project__c AS TEXT) AS "Project__c",
    CAST(id AS TEXT) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}