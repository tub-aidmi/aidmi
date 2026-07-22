{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(COALESCE(NULLIF(name, ''), 'Unknown'))) AS "Name",
    TRIM(serial_number__c) AS "Serial_Number__c",
    CASE
        WHEN warranty_end_date__c IS NULL OR TRIM(warranty_end_date__c) = '' THEN NULL
        WHEN TRIM(warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$' AND TRIM(warranty_end_date__c) != '0000-00-00' THEN TRIM(warranty_end_date__c)
        WHEN TRIM(warranty_end_date__c) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(warranty_end_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(warranty_end_date__c) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(warranty_end_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(warranty_end_date__c) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(warranty_end_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CASE
        WHEN account__c IS NULL OR TRIM(account__c) = '' THEN NULL
        ELSE TRIM(account__c)
    END AS "Account__c",
    CASE
        WHEN project__c IS NULL OR TRIM(project__c) = '' THEN NULL
        ELSE TRIM(project__c)
    END AS "Project__c",
    CAST(id AS TEXT) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}