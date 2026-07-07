-- depends_on: {{ ref('Account') }} -- depends_on: {{ ref('Project__c') }}

{{ config(materialized='table') }}

WITH source_asset AS (
    SELECT
        id,
        name,
        serial,
        warranty,
        client,
        project
    FROM {{ source('fixture_missing_relations_v2_src', 'asset') }}
),

source_account AS (
    SELECT
        id,
        name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
),

source_project AS (
    SELECT
        id
    FROM {{ source('fixture_missing_relations_v2_src', 'project') }}
)

SELECT
    sa.id AS "Id",
    COALESCE(NULLIF(TRIM(sa.name), ''), 'Unnamed Asset') AS "Name",
    sa.serial AS "Serial_Number__c",
    CASE
        WHEN sa.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(sa.warranty::DATE, 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(acc_id_match.id, acc_name_match.id) AS "Account__c",
    sp.id AS "Project__c",
    sa.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_asset AS sa
LEFT JOIN
    source_account AS acc_id_match ON sa.client = acc_id_match.id
LEFT JOIN
    source_account AS acc_name_match ON sa.client = acc_name_match.name
LEFT JOIN
    source_project AS sp ON sa.project = sp.id