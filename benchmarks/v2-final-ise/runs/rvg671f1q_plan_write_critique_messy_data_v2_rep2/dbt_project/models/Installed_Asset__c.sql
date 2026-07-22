{{ config(materialized='table') }}

SELECT 
    TRIM(UPPER(ia.id)) AS "Id",
    COALESCE(NULLIF(INITCAP(TRIM(ia.name)), ''), 'Unassigned Asset') AS "Name",
    UPPER(TRIM(ia.serial_number__c)) AS "Serial_Number__c",
    CASE 
        WHEN TRIM(ia.warranty_end_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(ia.warranty_end_date__c), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(ia.warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(ia.warranty_end_date__c), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(ia.warranty_end_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(ia.warranty_end_date__c), 'MM/DD/YYYY')::TEXT
        WHEN TRIM(ia.warranty_end_date__c) ~ '^\d{8}$' THEN TO_DATE(TRIM(ia.warranty_end_date__c), 'YYYYMMDD')::TEXT
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    COALESCE(TRIM(UPPER(acct.id)), TRIM(UPPER(ia.account__c))) AS "Account__c",
    TRIM(UPPER(ia.project__c)) AS "Project__c",
    ia.id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} ia
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} acct 
    ON TRIM(UPPER(ia.account__c)) = TRIM(UPPER(acct.id))