{{ config(materialized='table') }}

SELECT
    UPPER(TRIM(id)) AS "Id",
    INITCAP(TRIM(name)) AS "Name",
    TRIM(serial_number__c) AS "Serial_Number__c",
    CASE
        WHEN warranty_end_date__c IS NULL OR TRIM(warranty_end_date__c) = '' THEN NULL
        WHEN warranty_end_date__c ~ '^0{4}-0{2}-0{2}$' OR warranty_end_date__c = 'N/A' THEN NULL
        ELSE
            COALESCE(
                CASE WHEN warranty_end_date__c ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN TO_DATE(warranty_end_date__c, 'YYYY-MM-DD')::TEXT END,
                CASE WHEN warranty_end_date__c ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT END,
                CASE WHEN warranty_end_date__c ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')::TEXT END,
                CASE WHEN warranty_end_date__c ~ '^[0-9]{8}$' THEN TO_DATE(warranty_end_date__c, 'YYYYMMDD')::TEXT END
            )
    END AS "Warranty_End_Date__c",
    UPPER(TRIM(account__c)) AS "Account__c",
    UPPER(TRIM(project__c)) AS "Project__c",
    TRIM(id) AS "Legacy_Asset_ID__c",
    '1900-01-01' AS "CreatedDate",
    '1900-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}