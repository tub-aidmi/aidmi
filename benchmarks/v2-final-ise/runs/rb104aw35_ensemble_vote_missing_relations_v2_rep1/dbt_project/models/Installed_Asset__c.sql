{{ config(materialized='table') }}

WITH base AS (
    SELECT
        a.id AS asset_id,
        a.name AS asset_name,
        a.serial AS serial_number,
        a.warranty AS warranty_raw,
        a.client AS client_raw,
        a.project AS project_raw
    FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
),

-- Map account: client can be "ACC-XXXX" format or company name
account_mapping AS (
    SELECT
        id,
        name AS account_name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)

SELECT
    CAST(asset_id AS TEXT) AS "Id",
    asset_name AS "Name",
    serial_number AS "Serial_Number__c",
    CASE
        WHEN warranty_raw ~ '^\d{4}-\d{2}-\d{2}$' THEN warranty_raw
        WHEN warranty_raw ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(warranty_raw, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    -- Map Account: if client starts with ACC-, use directly; else match by company name
    CASE
        WHEN client_raw ~ '^ACC-\\d+$' THEN client_raw
        ELSE am.id
    END AS "Account__c",
    project_raw AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM base b
LEFT JOIN account_mapping am
    ON am.account_name = b.client_raw AND b.client_raw !~ '^ACC-\\d+$';