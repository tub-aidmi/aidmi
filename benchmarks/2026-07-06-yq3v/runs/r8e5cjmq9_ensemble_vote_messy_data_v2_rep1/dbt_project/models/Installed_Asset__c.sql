{{ config(materialized='table') }}

SELECT
    s.id AS "Id",
    COALESCE(s.name, 'Unknown Installed Asset') AS "Name",
    s.serial_number__c AS "Serial_Number__c",
    CASE
        WHEN s.warranty_end_date__c IS NULL OR TRIM(s.warranty_end_date__c) = '' THEN NULL
        WHEN s.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN s.warranty_end_date__c
        WHEN s.warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(s.warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN s.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(s.warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    s.account__c AS "Account__c",
    s.project__c AS "Project__c",
    s.id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS s
