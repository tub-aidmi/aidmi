{{ config(materialized='table') }}

SELECT
    ast.id AS "Id",
    COALESCE(ast.name, 'Unknown Asset') AS "Name",
    ast.serial AS "Serial_Number__c",
    CASE
        WHEN ast.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN ast.warranty::DATE::TEXT
        WHEN ast.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(ast.warranty, 'DD.MM.YYYY')::TEXT
        WHEN ast.warranty ~ '^\d{8}$' THEN TO_DATE(ast.warranty, 'YYYYMMDD')::TEXT
        WHEN ast.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(ast.warranty, 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acc.id AS "Account__c", -- Join to account table to get Salesforce-style Account Id
    ast.project AS "Project__c", -- Assuming project is the Salesforce Project Id
    ast.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS ast
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
ON
    ast.client = acc.id -- Assuming client maps to account.id
