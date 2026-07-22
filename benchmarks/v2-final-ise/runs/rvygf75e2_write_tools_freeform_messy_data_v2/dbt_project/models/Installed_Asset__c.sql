{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    TRIM(serial_number__c) AS "Serial_Number__c",
    CASE 
        WHEN TRIM(warranty_end_date__c) IS NULL OR TRIM(warranty_end_date__c) = '' OR warranty_end_date__c::TEXT = '0000-00-00' THEN NULL
        WHEN warranty_end_date__c::TEXT ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(warranty_end_date__c AS DATE)::TEXT
        WHEN warranty_end_date__c::TEXT ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')::TEXT
        WHEN warranty_end_date__c::TEXT ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT
        WHEN warranty_end_date__c::TEXT ~ '^\d{8}$' THEN 
            CAST(
                SUBSTR(warranty_end_date__c, 1, 4) || '-' || 
                SUBSTR(warranty_end_date__c, 5, 2) || '-' || 
                SUBSTR(warranty_end_date__c, 7, 2) AS DATE
             )::TEXT
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    TRIM(account__c) AS "Account__c",
    TRIM(project__c) AS "Project__c",
    TRIM(id) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
