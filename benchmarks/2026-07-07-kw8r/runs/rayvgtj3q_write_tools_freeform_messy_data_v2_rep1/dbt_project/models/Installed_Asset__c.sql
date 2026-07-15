{{ config(materialized='table') }}

SELECT
    id AS "Id",
    name AS "Name",
    serial_number__c AS "Serial_Number__c",
    CASE
        WHEN warranty_end_date__c = '0000-00-00' THEN NULL
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN warranty_end_date__c
        WHEN warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account__c AS "Account__c",
    project__c AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
