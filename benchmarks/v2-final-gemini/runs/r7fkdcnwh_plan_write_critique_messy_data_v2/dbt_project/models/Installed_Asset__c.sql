-- dbt model for Installed_Asset__c

{{ config(materialized='table') }}

SELECT
    TRIM(source.id) AS "Id",
    COALESCE(TRIM(source.name), 'N/A') AS "Name",
    TRIM(source.serial_number__c) AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN TRIM(source.warranty_end_date__c) IS NULL OR TRIM(source.warranty_end_date__c) = '' THEN NULL
            WHEN TRIM(source.warranty_end_date__c) = '0000-00-00' THEN NULL -- Explicitly handle this sentinel value
            ELSE COALESCE(
                -- YYYY-MM-DD format
                CASE WHEN source.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$'
                     THEN TO_DATE(source.warranty_end_date__c, 'YYYY-MM-DD')
                     ELSE NULL END,
                -- DD.MM.YYYY format
                CASE WHEN source.warranty_end_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
                     THEN TO_DATE(source.warranty_end_date__c, 'DD.MM.YYYY')
                     ELSE NULL END,
                -- MM/DD/YYYY format (handles M/D/YYYY as well)
                CASE WHEN source.warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                     THEN TO_DATE(source.warranty_end_date__c, 'MM/DD/YYYY')
                     ELSE NULL END,
                -- YYYYMMDD format
                CASE WHEN source.warranty_end_date__c ~ '^\d{8}$'
                     THEN TO_DATE(source.warranty_end_date__c, 'YYYYMMDD')
                     ELSE NULL END
            )
        END,
        'YYYY-MM-DD'
    ) AS "Warranty_End_Date__c",
    TRIM(source.account__c) AS "Account__c",
    TRIM(source.project__c) AS "Project__c",
    TRIM(source.id) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} AS source
