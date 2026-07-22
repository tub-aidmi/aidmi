{{ config(materialized='table') }}

SELECT
    CAST(id AS text) AS "Id",
    COALESCE(TRIM(name), 'Unknown Asset') AS "Name",
    serial_number__c AS "Serial_Number__c",
    -- Parse warranty date from various formats to ISO YYYY-MM-DD
    CASE
        WHEN warranty_end_date__c IS NULL THEN NULL
        WHEN warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$'
            THEN warranty_end_date__c
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account__c AS "Account__c",
    project__c AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}