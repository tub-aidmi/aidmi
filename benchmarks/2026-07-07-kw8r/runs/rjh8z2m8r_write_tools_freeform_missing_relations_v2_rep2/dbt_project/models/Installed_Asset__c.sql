{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(NULLIF(TRIM(a.name), ''), 'Unknown') AS "Name",
    TRIM(a.serial) AS "Serial_Number__c",
    CASE
        WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
        WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.warranty, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(
        (SELECT ac.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} ac WHERE ac.id = a.client),
        (SELECT ac.id FROM {{ source('fixture_missing_relations_v2_src', 'account') }} ac WHERE ac.name = a.client)
    ) AS "Account__c",
    COALESCE(
        (SELECT pr.id FROM {{ source('fixture_missing_relations_v2_src', 'project') }} pr WHERE pr.id = a.project),
        (SELECT pr.id FROM {{ source('fixture_missing_relations_v2_src', 'project') }} pr WHERE pr.name = a.project)
    ) AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
