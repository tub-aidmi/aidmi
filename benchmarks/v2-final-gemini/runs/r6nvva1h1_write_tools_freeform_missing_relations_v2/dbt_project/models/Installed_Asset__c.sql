{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Asset') AS "Name",
    serial AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN warranty ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN TO_DATE(warranty, 'YYYY-MM-DD')
            WHEN warranty ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' THEN TO_DATE(warranty, 'DD.MM.YYYY')
            WHEN warranty ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN TO_DATE(warranty, 'MM/DD/YYYY')
            WHEN warranty ~ '^\\d{8}$' THEN TO_DATE(warranty, 'YYYYMMDD')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Warranty_End_Date__c",
    client AS "Account__c", -- Assuming client is the Salesforce Account Id
    project AS "Project__c", -- Assuming project is the Salesforce Project Id
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }}
