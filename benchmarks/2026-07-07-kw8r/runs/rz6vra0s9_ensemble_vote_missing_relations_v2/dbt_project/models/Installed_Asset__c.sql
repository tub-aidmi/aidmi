{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    INITCAP(TRIM(a.name)) AS "Name",
    UPPER(TRIM(a.serial)) AS "Serial_Number__c",
    CASE
        WHEN a.warranty IS NOT NULL AND TRIM(a.warranty) != '' THEN
            CASE
                WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(a.warranty, 'YYYY-MM-DD')::TEXT
                WHEN a.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(a.warranty, 'MM/DD/YYYY')::TEXT
                WHEN a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.warranty, 'DD.MM.YYYY')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "Warranty_End_Date__c",
    a.client AS "Account__c",
    a.project AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a