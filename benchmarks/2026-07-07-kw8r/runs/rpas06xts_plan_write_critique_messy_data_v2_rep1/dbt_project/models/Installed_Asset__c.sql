{{ config(materialized='table') }}
WITH parsed_dates AS (
    SELECT
        id,
        name,
        serial_number__c,
        warranty_end_date__c,
        account__c,
        project__c,
        CASE
            WHEN warranty_end_date__c IS NULL THEN NULL
            WHEN warranty_end_date__c = '0000-00-00' THEN NULL
            WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(warranty_end_date__c, 'YYYY-MM-DD')
            WHEN warranty_end_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(warranty_end_date__c, 'MM/DD/YYYY')
            WHEN warranty_end_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(warranty_end_date__c, 'DD.MM.YYYY')
            WHEN warranty_end_date__c ~ '^\d{8}$' THEN TO_DATE(warranty_end_date__c, 'YYYYMMDD')
            ELSE NULL
        END AS parsed_warranty_end_date
    FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
)
SELECT
    ia.id AS "Id",
    COALESCE(INITCAP(TRIM(ia.name)), 'Unknown Asset') AS "Name",
    ia.serial_number__c AS "Serial_Number__c",
    CASE
        WHEN pd.parsed_warranty_end_date IS NOT NULL
        THEN TO_CHAR(pd.parsed_warranty_end_date, 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(acc.id, ia.account__c) AS "Account__c",
    COALESCE(prj.id, ia.project__c) AS "Project__c",
    ia.id AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM parsed_dates pd
JOIN {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} ia ON pd.id = ia.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} acc ON ia.account__c = acc.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'project__c') }} prj ON ia.project__c = prj.id