{{ config(materialized='table') }}

WITH asset_with_account AS (
    SELECT 
        a.*,
        COALESCE(
            acc1.id,
            acc2.id
        ) AS account_id
    FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
    LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc1 
        ON a.client = acc1.id
    LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc2 
        ON TRIM(a.client) = TRIM(acc2.name)
)

SELECT
    a.id AS "Id",
    COALESCE(NULLIF(TRIM(a.name), ''), 'Unknown') AS "Name",
    NULLIF(TRIM(a.serial), '') AS "Serial_Number__c",
    CASE 
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
        ELSE NULL
    END AS "Warranty_End_Date__c",
    awa.account_id AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM asset_with_account awa
JOIN {{ source('fixture_missing_relations_v2_src', 'asset') }} a ON awa.id = a.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p 
    ON a.project = p.id
WHERE awa.account_id IS NOT NULL AND p.id IS NOT NULL
