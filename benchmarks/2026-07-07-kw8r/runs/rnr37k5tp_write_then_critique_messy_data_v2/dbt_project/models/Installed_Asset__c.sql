{{ config(materialized='table') }}

SELECT
    ia.id AS "Id",
    COALESCE(TRIM(ia.name), 'Unknown Asset') AS "Name",
    TRIM(ia.serial_number__c) AS "Serial_Number__c",
      -- Warranty end date: parse multiple formats into ISO YYYY-MM-DD, return NULL for invalid/sentinel values
    CASE
        WHEN TRIM(ia.warranty_end_date__c) IS NULL OR LOWER(TRIM(ia.warranty_end_date__c)) IN ('n/a', 'null', '') THEN NULL
        WHEN TRIM(ia.warranty_end_date__c) = '0000-00-00' THEN NULL
          -- ISO 8601: YYYY-MM-DD (pass through if valid)
        WHEN TRIM(ia.warranty_end_date__c) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN CAST(TO_DATE(TRIM(ia.warranty_end_date__c), 'YYYY-MM-DD') AS text)
          -- European: DD.MM.YYYY (day can be 1 or 2 digits, month always 2 digits)
        WHEN TRIM(ia.warranty_end_date__c) ~ '^\d{1,2}\.\d{2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(ia.warranty_end_date__c), 'DD.MM.YYYY'), 'YYYY-MM-DD')
          -- US: MM/DD/YYYY (month and day can be 1 or 2 digits)
        WHEN TRIM(ia.warranty_end_date__c) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(ia.warranty_end_date__c), 'MM/DD/YYYY'), 'YYYY-MM-DD')
          -- Compact: YYYYMMDD (exactly 8 digits)
        WHEN TRIM(ia.warranty_end_date__c) ~ '^\d{8}$'
            THEN TO_CHAR(TO_DATE(TRIM(ia.warranty_end_date__c), 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
      -- Account mapping: LEFT JOIN account to resolve to canonical Account Id with consistent normalization; fallback to raw source key
    COALESCE(TRIM(acc.id), TRIM(ia.account__c)) AS "Account__c",
    TRIM(ia.project__c) AS "Project__c",
      -- Legacy asset ID preserved for row-level verification
    CAST(ia.id AS text) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} ia
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} acc
    ON TRIM(LOWER(ia.account__c)) = TRIM(LOWER(acc.id))