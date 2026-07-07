{{ config(materialized='table') }}

SELECT
    TRIM(T1.id) AS "Id",
    COALESCE(TRIM(T1.name), 'Unknown Asset') AS "Name",
    TRIM(T1.serial_number__c) AS "Serial_Number__c",
    CASE
        WHEN TRIM(T1.warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(T1.warranty_end_date__c), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(T1.warranty_end_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(T1.warranty_end_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(T1.warranty_end_date__c) ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(T1.warranty_end_date__c), 'MM-DD-YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(T1.warranty_end_date__c) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(T1.warranty_end_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(T1.account__c) AS "Account__c",
    TRIM(T1.project__c) AS "Project__c",
    TRIM(T1.id) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS T1