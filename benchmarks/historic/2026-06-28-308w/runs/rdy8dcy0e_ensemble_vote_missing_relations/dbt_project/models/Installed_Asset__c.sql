
{{ config(materialized='table') }}

WITH source_asset AS (
    SELECT * FROM {{ source('fixture_missing_relations_src', 'Asset') }}
),
source_account AS (
    SELECT * FROM {{ source('fixture_missing_relations_src', 'Account') }}
),
source_project AS (
    SELECT * FROM {{ source('fixture_missing_relations_src', 'Project') }}
)
SELECT
    sa.id AS "Id",
    COALESCE(sa.name, 'Unknown Asset Name') AS "Name",
    sa.serial AS "Serial_Number__c",
    CASE
        WHEN sa.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN sa.warranty
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(account_by_id.id, account_by_name.id) AS "Account__c",
    sp.id AS "Project__c",
    sa.id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_asset AS sa
LEFT JOIN
    source_account AS account_by_id
    ON sa.client = account_by_id.id
LEFT JOIN
    source_account AS account_by_name
    ON sa.client = account_by_name.name
LEFT JOIN
    source_project AS sp
    ON sa.project = sp.id
