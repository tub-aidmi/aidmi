{{
    config(materialized='table')
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
    COALESCE(
        TO_CHAR(TO_DATE(warranty, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(warranty, 'YYYYMMDD'), 'YYYY-MM-DD'),
        NULL
    ) AS "Warranty_End_Date__c",
    client AS "Account__c",
    project AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_data