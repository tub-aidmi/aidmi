{{ config(materialized='table') }}
WITH resolved_account AS (
    SELECT 
        a.id AS asset_id,
        COALESCE(
            (SELECT acc.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} acc WHERE a.client = acc.id LIMIT 1),
            (SELECT acc.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} acc WHERE a.client = acc.name LIMIT 1)
        ) AS account_id
    FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
)
SELECT 
    a.id AS "Id",
    INITCAP(a.name) AS "Name",
    UPPER(a.serial) AS "Serial_Number__c",
    CASE 
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    ra.account_id AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN resolved_account ra ON a.id = ra.asset_id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p ON a.project = p.id