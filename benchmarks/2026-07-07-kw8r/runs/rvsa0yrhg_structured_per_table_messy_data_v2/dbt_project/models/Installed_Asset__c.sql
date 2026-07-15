{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
)
SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unknown Asset') AS "Name",
    serial_number__c AS "Serial_Number__c",
    CASE
        -- ISO format: YYYY-MM-DD (exclude sentinel 0000-00-00)
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$'
             AND warranty_end_date__c != '0000-00-00' THEN TO_DATE(warranty_end_date__c, 'YYYY-MM-DD')::TEXT
        -- US format: M/D/YYYY or MM/DD/YYYY
        WHEN warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')::TEXT
        -- European format: D.MM.YYYY or DD.MM.YYYY
        WHEN warranty_end_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT
        -- Compact format: YYYYMMDD
        WHEN warranty_end_date__c ~ '^\d{8}$' THEN TO_DATE(warranty_end_date__c, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account__c AS "Account__c",
    project__c AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source