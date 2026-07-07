-- dbt model for Installed_Asset__c

{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'N/A') AS "Name",
    TRIM(serial_number__c) AS "Serial_Number__c",
    CASE
        WHEN TRIM(warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(warranty_end_date__c), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(warranty_end_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(warranty_end_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(warranty_end_date__c) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(warranty_end_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(warranty_end_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(warranty_end_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account__c AS "Account__c",
    project__c AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
