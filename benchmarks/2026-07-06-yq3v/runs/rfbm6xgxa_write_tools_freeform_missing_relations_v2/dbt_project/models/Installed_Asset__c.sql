{{
  config(
    materialized='table'
  )
}}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Asset') AS "Name",
    serial AS "Serial_Number__c",
    CASE
        WHEN warranty IS NULL OR TRIM(warranty) = '' THEN NULL
        WHEN warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN warranty -- Already YYYY-MM-DD
        WHEN warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN warranty ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(warranty, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL -- Unparseable format
    END AS "Warranty_End_Date__c",
    client AS "Account__c", -- Assuming client maps to Account.Id
    project AS "Project__c", -- Assuming project maps to Project.Id
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }}
