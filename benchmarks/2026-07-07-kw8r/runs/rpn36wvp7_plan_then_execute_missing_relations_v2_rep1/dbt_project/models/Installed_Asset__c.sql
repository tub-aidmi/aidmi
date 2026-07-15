{{ config(materialized='table') }}

SELECT 
    ast.id AS "Id",
    ast.name AS "Name",
    ast.serial AS "Serial_Number__c",
    CASE 
        WHEN TRIM(ast.warranty) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(ast.warranty), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(ast.warranty) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(ast.warranty), 'DD.MM.YYYY')::TEXT
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    a.id AS "Account__c",
    p.id AS "Project__c",
    ast.id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    CAST(0 AS INTEGER) AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} ast
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(ast.client) = TRIM(a.id)
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p 
    ON TRIM(ast.project) = TRIM(p.id)