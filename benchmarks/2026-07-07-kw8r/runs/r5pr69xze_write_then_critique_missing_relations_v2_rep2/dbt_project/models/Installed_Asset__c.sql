{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(a.name, 'Unknown') AS "Name",
    a.serial AS "Serial_Number__c",
    CASE
        WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
        WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
            TO_CHAR(TO_DATE(a.warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{8}$' THEN
            TO_CHAR(TO_DATE(a.warranty, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(acc_direct.id, acc_name.id) AS "Account__c",
    proj.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc_direct
    ON a.client LIKE 'ACC-%'
    AND acc_direct.id = a.client
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc_name
    ON a.client NOT LIKE 'ACC-%'
    AND LOWER(TRIM(acc_name.name)) = LOWER(TRIM(a.client))
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} proj
    ON a.project = proj.id