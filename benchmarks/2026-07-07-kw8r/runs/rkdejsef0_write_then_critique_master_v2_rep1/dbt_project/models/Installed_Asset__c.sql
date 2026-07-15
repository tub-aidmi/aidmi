{{ config(materialized='table') }}

SELECT 
     -- Id: Salesforce-style 18-char Id = 'A' (asset prefix) + asset_kennnung padded to 15 chars
    ('A' || LPAD(TRIM(asset_kennung), 15, '0')) AS "Id",

     -- Name: INITCAP and trim whitespace
    INITCAP(TRIM(asset_name)) AS "Name",

     -- Serial number: trimmed for consistency
    TRIM(serien_nummer) AS "Serial_Number__c",

     -- Warranty date: parse DD.MM.YYYY or YYYY-MM-DD with regex guards to avoid TO_DATE errors
    CASE
        WHEN TRIM(garantieende) IS NOT NULL AND TRIM(garantieende) != ''
             AND TRIM(garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$'
        THEN TO_CHAR(TO_DATE(TRIM(garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')

        WHEN TRIM(garantieende) IS NOT NULL AND TRIM(garantieende) != ''
             AND TRIM(garantieende) ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_CHAR(TO_DATE(TRIM(garantieende), 'YYYY-MM-DD'), 'YYYY-MM-DD')

        ELSE NULL
    END AS "Warranty_End_Date__c",

     -- Account__c: Salesforce-style 18-char Id = '001' (Account prefix) + kunden_kennnung padded to 15 chars
    ('001' || LPAD(TRIM(kunden_kennung), 15, '0')) AS "Account__c",

     -- Project__c: Salesforce-style 18-char Id = 'P00' (Project prefix) + projekt_kennnung padded to 15 chars
    ('P00' || LPAD(TRIM(projekt_kennung), 15, '0')) AS "Project__c",

     -- Legacy_Asset_ID__c: raw source natural key for row-level traceability
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",

     -- Synthetic timestamps since source data lacks creation/modification dates
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",

     -- IsDeleted: 0 for all active rows
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }}

WHERE TRIM(asset_name) IS NOT NULL
   OR TRIM(asset_kennung) IS NOT NULL