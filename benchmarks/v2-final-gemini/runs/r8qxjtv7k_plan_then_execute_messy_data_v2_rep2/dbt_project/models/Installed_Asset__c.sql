{{ config(materialized='table') }}

SELECT
    TRIM(src.id) AS "Id",
    TRIM(COALESCE(src.name, 'Unknown Asset')) AS "Name",
    TRIM(src.serial_number__c) AS "Serial_Number__c",
    CASE
        WHEN src.warranty_end_date__c IS NULL THEN NULL
        WHEN src.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN src.warranty_end_date__c
        WHEN src.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(src.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN src.warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src.warranty_end_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(src.warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(src.account__c) AS "Account__c",
    TRIM(src.project__c) AS "Project__c",
    TRIM(src.id) AS "Legacy_Asset_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS src
