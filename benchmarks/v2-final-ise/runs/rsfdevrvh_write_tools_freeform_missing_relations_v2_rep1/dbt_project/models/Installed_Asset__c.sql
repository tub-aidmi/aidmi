{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(NULLIF(a.name, ''), 'Unknown') AS "Name",
    a.serial AS "Serial_Number__c",
    CASE
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
        WHEN a.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.warranty, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acc.id AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON a.client = acc.id OR a.client = acc.name
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p
    ON a.project = p.id OR a.project = p.name
