{{ config(materialized='table') }}

SELECT 
    a.id AS "Id",
    COALESCE(a.name, 'Asset - ' || a.serial) AS "Name",
    a.serial AS "Serial_Number__c",
    -- Parse warranty date from common formats; prefer NULL for unparseable dates
    CASE 
        WHEN a.warranty IS NOT NULL AND TRIM(a.warranty) = '' THEN NULL
        WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' 
            THEN TO_DATE(TRIM(a.warranty), 'DD.MM.YYYY')::TEXT
        WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN TRIM(a.warranty)
        WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{2}/\d{2}/\d{4}$' 
            THEN TO_DATE(TRIM(a.warranty), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    -- Account__c: join asset to source account via client field to get proper Salesforce-style Account Id
    acct.id AS "Account__c",
    -- Project__c: pass through the project reference from the source asset
    a.project AS "Project__c",
    -- Legacy_Asset_ID__c: populate from source asset id for row-level verification
    a.id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acct 
    ON a.client = acct.id