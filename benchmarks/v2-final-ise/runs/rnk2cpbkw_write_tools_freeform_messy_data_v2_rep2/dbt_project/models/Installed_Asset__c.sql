{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        id,
        name,
        serial_number__c,
        warranty_end_date__c,
        account__c,
        project__c
    FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
),

transformed AS (
    SELECT
        id AS "Id",
        COALESCE(TRIM(INITCAP(name)), '') AS "Name",
        TRIM(serial_number__c) AS "Serial_Number__c",
        CASE
            WHEN warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT
            WHEN warranty_end_date__c ~ '^\d{8}$' THEN SUBSTR(warranty_end_date__c, 1, 4) || '-' || SUBSTR(warranty_end_date__c, 5, 2) || '-' || SUBSTR(warranty_end_date__c, 7, 2)
            WHEN warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')::TEXT
            ELSE NULL
        END AS "Warranty_End_Date__c",
        CAST(account__c AS TEXT) AS "Account__c",
        CAST(project__c AS TEXT) AS "Project__c",
        id AS "Legacy_Asset_ID__c",
        CURRENT_DATE::TEXT AS "CreatedDate",
        CURRENT_DATE::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"
    FROM source_data
)

SELECT * FROM transformed
