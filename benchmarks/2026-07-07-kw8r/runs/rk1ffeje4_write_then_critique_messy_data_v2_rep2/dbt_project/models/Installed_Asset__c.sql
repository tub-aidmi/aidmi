{{ config(materialized='table') }}

SELECT
    -- Primary key (mapped directly from source)
    CAST(id AS TEXT) AS "Id",

    -- Name: NOT NULL, use name with sensible default
    COALESCE(TRIM(name), 'Unknown Asset') AS "Name",

    -- Serial number
    serial_number__c AS "Serial_Number__c",

    -- Warranty end date: parse multiple formats, return ISO YYYY-MM-DD or NULL
    CASE
        WHEN warranty_end_date__c IS NULL
             OR warranty_end_date__c = 'N/A'
             OR warranty_end_date__c = '0000-00-00' THEN NULL
        -- ISO format: 2029-05-02
        WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_DATE(warranty_end_date__c, 'YYYY-MM-DD')::TEXT
        -- Compact numeric: 20291103
        WHEN warranty_end_date__c ~ '^\d{8}$'
            THEN TO_DATE(warranty_end_date__c, 'YYYYMMDD')::TEXT
        -- European dot-separated: 27.02.2030
        WHEN warranty_end_date__c ~ '^[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$'
            THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')::TEXT
        -- US slash-separated: 7/9/2030 or 12/20/2028
        WHEN warranty_end_date__c ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
            THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account reference: source account__c already uses Salesforce-style id format (CUST-XXXX)
    CAST(account__c AS TEXT) AS "Account__c",

    -- Project reference: source project__c already uses Salesforce-style id format (PROJ-XXXXX)
    CAST(project__c AS TEXT) AS "Project__c",

    -- Legacy asset ID from source natural key
    CAST(id AS TEXT) AS "Legacy_Asset_ID__c",

    -- Derived / synthetic fields
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}