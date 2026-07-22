SELECT 
    TRIM(UPPER(id)) AS "Id",
    INITCAP(TRIM(COALESCE(name, 'Unknown'))) AS "Name",
    TRIM(serial_number__c) AS "Serial_Number__c",
    CASE 
        WHEN TRIM(warranty_end_date__c) IS NULL OR TRIM(warranty_end_date__c) = '' THEN NULL
        WHEN TRIM(warranty_end_date__c) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(warranty_end_date__c) ~ '^\d{8}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'YYYYMMDD')::TEXT
        WHEN TRIM(warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(warranty_end_date__c) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(warranty_end_date__c), 'MM/DD/YYYY')::TEXT
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    NULLIF(TRIM(UPPER(account__c)), '') AS "Account__c",
    NULLIF(TRIM(UPPER(project__c)), '') AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}