-- This dbt model transforms raw installed asset data into the target Installed_Asset__c schema.

{{ config(materialized='table') }}

SELECT
    src.id AS "Id",
    COALESCE(TRIM(src.name), 'Unknown Asset') AS "Name",
    TRIM(src.serial_number__c) AS "Serial_Number__c",
    CASE
        WHEN src.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(src.warranty_end_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN src.warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(src.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN src.warranty_end_date__c ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_CHAR(TO_DATE(src.warranty_end_date__c, 'DD-MM-YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    src.account__c AS "Account__c",
    src.project__c AS "Project__c",
    src.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} src