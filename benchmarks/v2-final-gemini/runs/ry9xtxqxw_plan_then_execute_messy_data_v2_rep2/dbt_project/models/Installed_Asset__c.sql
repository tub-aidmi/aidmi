{{ config(materialized='table') }}

SELECT
    installed_asset__c.id AS "Id",
    COALESCE(TRIM(installed_asset__c.name), 'Unknown Asset ' || installed_asset__c.id) AS "Name",
    installed_asset__c.serial_number__c AS "Serial_Number__c",
    CASE
        WHEN installed_asset__c.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(installed_asset__c.warranty_end_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN installed_asset__c.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(installed_asset__c.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN installed_asset__c.warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(installed_asset__c.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    installed_asset__c.account__c AS "Account__c",
    installed_asset__c.project__c AS "Project__c",
    installed_asset__c.id AS "Legacy_Asset_ID__c",
    CURRENT_DATE AS "CreatedDate",
    CURRENT_DATE AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS installed_asset__c
