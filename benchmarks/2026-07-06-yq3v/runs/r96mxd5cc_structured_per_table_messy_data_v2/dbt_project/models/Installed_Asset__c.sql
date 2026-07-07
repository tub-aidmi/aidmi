-- dbt model for Installed_Asset__c
{{ config(materialized='table') }}

SELECT
    s.id AS "Id",
    COALESCE(s.name, 'Untitled Asset') AS "Name",
    s.serial_number__c AS "Serial_Number__c",
    CASE
        WHEN s.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' AND s.warranty_end_date__c != '0000-00-00' THEN TO_CHAR(TO_DATE(s.warranty_end_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN s.warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(s.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN s.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(s.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN s.warranty_end_date__c ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(s.warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    s.account__c AS "Account__c",
    s.project__c AS "Project__c",
    s.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS s