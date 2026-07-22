{{ config(materialized='table') }}

SELECT
    TRIM(src.id) AS "Id",
    COALESCE(TRIM(src.name), 'Unknown Asset Name') AS "Name",
    TRIM(src.serial_number__c) AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN TRIM(src.warranty_end_date__c) = '' THEN NULL
            WHEN TRIM(src.warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(src.warranty_end_date__c), 'YYYY-MM-DD')
            WHEN TRIM(src.warranty_end_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(src.warranty_end_date__c), 'DD.MM.YYYY')
            WHEN TRIM(src.warranty_end_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(src.warranty_end_date__c), 'MM/DD/YYYY')
            WHEN TRIM(src.warranty_end_date__c) ~ '^\d{8}$' THEN TO_DATE(TRIM(src.warranty_end_date__c), 'YYYYMMDD')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Warranty_End_Date__c",
    TRIM(src.account__c) AS "Account__c",
    TRIM(src.project__c) AS "Project__c",
    TRIM(src.id) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} src
