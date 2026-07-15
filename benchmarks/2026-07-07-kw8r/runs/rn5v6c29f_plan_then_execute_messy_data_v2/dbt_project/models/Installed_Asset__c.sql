{{ config(materialized='table') }}

WITH account_map AS (
    SELECT 
        LOWER(TRIM(id)) AS normalized_id,
        TRIM(id) AS target_id
    FROM {{ source('fixture_messy_data_v2_src', 'account') }}
),
project_map AS (
    SELECT 
        LOWER(TRIM(id)) AS normalized_id,
        TRIM(id) AS target_id
    FROM {{ source('fixture_messy_data_v2_src', 'project__c') }}
)

SELECT 
    COALESCE(TRIM(ia.id), '') AS "Id",
    COALESCE(INITCAP(TRIM(ia.name)), 'Unknown') AS "Name",
    TRIM(ia.serial_number__c) AS "Serial_Number__c",
    CASE 
        WHEN ia.warranty_end_date__c IS NULL OR TRIM(ia.warranty_end_date__c) = '' THEN NULL
        WHEN ia.warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(ia.warranty_end_date__c), 'YYYY-MM-DD')::TEXT
        WHEN ia.warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(ia.warranty_end_date__c), 'DD.MM.YYYY')::TEXT
        WHEN ia.warranty_end_date__c ~ '^\d{8}$' THEN TO_DATE(TRIM(ia.warranty_end_date__c), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    ac.target_id AS "Account__c",
    pj.target_id AS "Project__c",
    TRIM(ia.id) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} ia
LEFT JOIN account_map ac 
    ON LOWER(TRIM(ia.account__c)) = ac.normalized_id
LEFT JOIN project_map pj 
    ON LOWER(TRIM(ia.project__c)) = pj.normalized_id