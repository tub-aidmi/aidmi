{{ config(materialized='table') }}

WITH normalized_assets AS (
    SELECT
        id,
        name,
        serial_number__c,
        warranty_end_date__c,
        account__c,
        project__c
    FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
)

SELECT
    INITCAP(TRIM(id)) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    UPPER(TRIM(serial_number__c)) AS "Serial_Number__c",
    /* Robust multi-format date parser for Warranty_End_Date__c */
    CASE
        WHEN warranty_end_date__c IS NULL OR TRIM(warranty_end_date__c) = '' THEN NULL
        WHEN TRIM(UPPER(warranty_end_date__c)) IN ('N/A', 'NULL', 'NA') THEN NULL
        WHEN TRIM(warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$'
            AND TRIM(warranty_end_date__c) != '0000-00-00'
            THEN TRIM(warranty_end_date__c)  /* Already ISO YYYY-MM-DD */
        WHEN TRIM(warranty_end_date__c) ~ '^\d{8}$'
            THEN TO_DATE(TRIM(warranty_end_date__c), 'YYYYMMDD')::TEXT
        WHEN TRIM(warranty_end_date__c) ~ '^\d+\.\d+\.\d{4}$'
            THEN TO_DATE(TRIM(warranty_end_date__c), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(warranty_end_date__c) ~ '^\d+/\d+/\d{4}$'
            THEN TO_DATE(TRIM(warranty_end_date__c), 'MM/DD/YYYY')::TEXT
        ELSE NULL  /* Unparseable or sentinel dates → NULL */
    END AS "Warranty_End_Date__c",
    INITCAP(TRIM(account__c)) AS "Account__c",
    INITCAP(TRIM(project__c)) AS "Project__c",
    TRIM(id) AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM normalized_assets