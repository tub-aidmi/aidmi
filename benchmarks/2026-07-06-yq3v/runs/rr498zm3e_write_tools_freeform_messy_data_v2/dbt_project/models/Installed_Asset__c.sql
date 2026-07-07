{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    TRIM(serial_number__c) AS "Serial_Number__c",
    CASE
        WHEN TRIM(warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN -- YYYY-MM-DD
            TRIM(warranty_end_date__c)
        WHEN TRIM(warranty_end_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN -- DD.MM.YYYY
            TO_CHAR(TO_DATE(TRIM(warranty_end_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(warranty_end_date__c) ~ '^\d{8}$' THEN -- YYYYMMDD
            TO_CHAR(TO_DATE(TRIM(warranty_end_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(warranty_end_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN -- MM/DD/YYYY
            TO_CHAR(TO_DATE(TRIM(warranty_end_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(project__c) AS "Project__c",
    id AS "Legacy_Asset_ID__c", -- Using source id as legacy ID
    NULL AS "CreatedDate", -- Not available in source
    NULL AS "LastModifiedDate", -- Not available in source
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
