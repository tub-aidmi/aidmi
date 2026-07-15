{{ config(materialized='table') }}

WITH parsed_dates AS (
    SELECT
        id,
        name,
        serial_number__c,
        account__c,
        project__c,
        CASE
            WHEN warranty_end_date__c IS NULL OR TRIM(warranty_end_date__c) = '' THEN NULL
            WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' AND warranty_end_date__c != '0000-00-00' THEN warranty_end_date__c
            WHEN warranty_end_date__c ~ '^\d{8}$' THEN
                SUBSTR(warranty_end_date__c, 1, 4) || '-' || SUBSTR(warranty_end_date__c, 5, 2) || '-' || SUBSTR(warranty_end_date__c, 7, 2)
            WHEN warranty_end_date__c ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN
                LPAD(SPLIT_PART(warranty_end_date__c, '/', 3), 4, '0') || '-' ||
                LPAD(SPLIT_PART(warranty_end_date__c, '/', 1), 2, '0') || '-' ||
                LPAD(SPLIT_PART(warranty_end_date__c, '/', 2), 2, '0')
            WHEN warranty_end_date__c ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN
                SUBSTR(warranty_end_date__c, 7, 4) || '-' ||
                LPAD(SPLIT_PART(warranty_end_date__c, '.', 2), 2, '0') || '-' ||
                LPAD(SPLIT_PART(warranty_end_date__c, '.', 1), 2, '0')
            ELSE NULL
        END AS warranty_end_date
    FROM "fixture_messy_data_v2_src"."installed_asset__c"
)

SELECT
    id AS "Id",
    COALESCE(TRIM(name), '') AS "Name",
    serial_number__c AS "Serial_Number__c",
    warranty_end_date AS "Warranty_End_Date__c",
    account__c AS "Account__c",
    project__c AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM parsed_dates
