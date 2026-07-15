{{ config(materialized='table') }}

SELECT
    CAST(a.id AS TEXT) AS "Id",
    a.name AS "Name",
    a.serial AS "Serial_Number__c",
    CASE
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(a.warranty, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty ~ '^\d{8}$' THEN SUBSTR(a.warranty, 1, 4) || '-' || SUBSTR(a.warranty, 5, 2) || '-' || SUBSTR(a.warranty, 7, 2)
        ELSE NULL
    END AS "Warranty_End_Date__c",
    -- Map client to Account Id by looking up the account name in the account source
    CASE
        WHEN a.client ~ '^ACC-\d+$' THEN a.client
        ELSE NULL
    END AS "Account__c",
    CASE
        WHEN a.project ~ '^PROJ-\d+$' THEN a.project
        ELSE NULL
    END AS "Project__c",
    CAST(a.id AS TEXT) AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
