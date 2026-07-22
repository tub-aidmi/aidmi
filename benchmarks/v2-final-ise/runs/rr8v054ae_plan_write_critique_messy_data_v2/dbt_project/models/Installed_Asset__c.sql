{{ config(materialized='table') }}

WITH parsed_dates AS (
    SELECT
        id,
        warranty_end_date__c,
        CASE
            WHEN warranty_end_date__c IS NULL THEN NULL
            WHEN warranty_end_date__c = '0000-00-00' THEN NULL
            WHEN warranty_end_date__c ~ '^\d{4}-\d{2}-\d{2}$' THEN 
                TO_CHAR(TO_DATE(warranty_end_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN warranty_end_date__c ~ '^\d{2}/\d{2}/\d{4}$' THEN 
                TO_CHAR(TO_DATE(warranty_end_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN warranty_end_date__c ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
                TO_CHAR(TO_DATE(warranty_end_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN warranty_end_date__c ~ '^\d{8}$' THEN 
                TO_CHAR(TO_DATE(warranty_end_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS parsed_warranty_end_date
    FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }}
)

SELECT
    ia.id AS "Id",
    COALESCE(ia.name, 'Unknown') AS "Name",
    ia.serial_number__c AS "Serial_Number__c",
    pd.parsed_warranty_end_date AS "Warranty_End_Date__c",
    a.id AS "Account__c",
    p.id AS "Project__c",
    ia.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'installed_asset__c') }} ia
LEFT JOIN parsed_dates pd ON ia.id = pd.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON ia.account__c = a.id
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'project__c') }} p ON ia.project__c = p.id