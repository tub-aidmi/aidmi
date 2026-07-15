{{ config(materialized='table') }}

WITH parsed_dates AS (
    SELECT
        -- Handle various date formats and return ISO YYYY-MM-DD or NULL
        CASE
            WHEN warranty_end_date__c IS NULL THEN NULL
            WHEN TRIM(warranty_end_date__c) IN ('N/A', '0000-00-00') THEN NULL
            -- YYYYMMDD format (8 digits, no separators)
            WHEN warranty_end_date__c ~ '^\d{8}$' THEN TO_DATE(warranty_end_date__c, 'YYYYMMDD')::TEXT
            -- ISO format YYYY-MM-DD with valid date
            WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$'
                 AND (TO_DATE(warranty_end_date__c, 'YYYY-MM-DD') IS NOT NULL) THEN TO_DATE(warranty_end_date__c, 'YYYY-MM-DD')::TEXT
            -- DD.MM.YYYY format (European dots)
            WHEN warranty_end_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT
            -- MM/DD/YYYY format (US slashes) - first part is month (1-12), second is day (1-31)
            WHEN warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')::TEXT
            ELSE NULL
        END AS "Warranty_End_Date__c",
        id AS "Id",
        name AS "Name",
        serial_number__c AS "Serial_Number__c",
        account__c AS "Account__c",
        project__c AS "Project__c",
        id AS "Legacy_Asset_ID__c"
    FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
)

SELECT
    "Id",
    "Name",
    "Serial_Number__c",
    "Warranty_End_Date__c",
    "Account__c",
    "Project__c",
    "Legacy_Asset_ID__c",
    -- Static metadata columns with sensible defaults
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM parsed_dates;