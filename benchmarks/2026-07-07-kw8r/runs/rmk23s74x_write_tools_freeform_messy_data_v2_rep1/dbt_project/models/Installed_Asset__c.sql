{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    TRIM(serial_number__c) AS "Serial_Number__c",
    CASE
        WHEN TRIM(warranty_end_date__c) IS NULL OR TRIM(warranty_end_date__c) = '' THEN NULL
        WHEN TRIM(warranty_end_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(warranty_end_date__c)
        WHEN TRIM(warranty_end_date__c) ~ '^\d{8}$' THEN SUBSTR(TRIM(warranty_end_date__c), 1, 4) || '-' || SUBSTR(TRIM(warranty_end_date__c), 5, 2) || '-' || SUBSTR(TRIM(warranty_end_date__c), 7, 2)
        WHEN TRIM(warranty_end_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(warranty_end_date__c) ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'DD-MM-YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(project__c) AS "Project__c",
    CAST(id AS TEXT) AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
