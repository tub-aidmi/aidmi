{{ config(materialized='table') }}

SELECT
    TRIM(aaa.id) AS "Id",
    COALESCE(TRIM(INITCAP(aaa.name)), 'Unnamed Asset') AS "Name",
    TRIM(UPPER(aaa.serial_number__c)) AS "Serial_Number__c",
    CASE 
        WHEN aaa.warranty_end_date__c IS NULL OR TRIM(aaa.warranty_end_date__c) = '' THEN NULL
        WHEN aaa.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}' THEN TO_DATE(TRIM(aaa.warranty_end_date__c), 'YYYY-MM-DD')::TEXT
        WHEN aaa.warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}' THEN TO_DATE(TRIM(aaa.warranty_end_date__c), 'MM/DD/YYYY')::TEXT
        WHEN aaa.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}' THEN TO_DATE(TRIM(aaa.warranty_end_date__c), 'DD.MM.YYYY')::TEXT
        WHEN aaa.warranty_end_date__c ~ '^\d{8}' AND LENGTH(TRIM(aaa.warranty_end_date__c)) = 8 THEN TO_DATE(TRIM(aaa.warranty_end_date__c), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acct.id AS "Account__c",
    proj.id AS "Project__c",
    TRIM(aaa.id) AS "Legacy_Asset_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} aaa
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} acct
    ON TRIM(aaa.account__c) = TRIM(acct.id)
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'project__c') }} proj
    ON TRIM(aaa.project__c) = TRIM(proj.id)