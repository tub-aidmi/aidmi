{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        id,
        name,
        serial,
        warranty,
        client,
        project
    FROM
        {{ source('fixture_missing_relations_v2_src', 'asset') }}
)

SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Asset') AS "Name",
    serial AS "Serial_Number__c",
    CASE
        WHEN warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN warranty::DATE::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    client AS "Account__c", -- Assuming client maps to Account.Id
    project AS "Project__c", -- Assuming project maps to Project__c.Id
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_data