{{ config(materialized='table') }}

-- Pre-compute unique name-to-account mapping to prevent duplicate rows in the fallback name join
WITH account_name_map AS (
    SELECT DISTINCT ON (LOWER(TRIM(name))) 
        LOWER(TRIM(name)) AS lower_name,
        id AS account_id
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
    ORDER BY LOWER(TRIM(name)), id
),

normalized_assets AS (
    SELECT
        a.id,
        a.name,
        a.serial,
        CASE
            WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
            WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
                TO_DATE(a.warranty, 'DD.MM.YYYY')::TEXT
            WHEN a.warranty IS NOT NULL AND a.warranty ~ '^\d{8}$' THEN
                TO_DATE(a.warranty, 'YYYYMMDD')::TEXT
            ELSE NULL
        END AS warranty_parsed,
        a.client,
        a.project
    FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
)

SELECT
    na.id AS "Id",
    na.name AS "Name",
    TRIM(na.serial) AS "Serial_Number__c",
    na.warranty_parsed AS "Warranty_End_Date__c",
    COALESCE(acc_id.account_id, acc_name.account_id) AS "Account__c",
    proj.id AS "Project__c",
    na.id AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM normalized_assets na

-- Direct ACC-prefixed ID match: highest priority when client is already an account id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc_id
    ON na.client LIKE 'ACC-%'
    AND acc_id.id = na.client

-- Fallback name match using pre-resolved unique map (at most one row per asset)
LEFT JOIN account_name_map acc_name
    ON LOWER(TRIM(na.client)) = acc_name.lower_name

-- Project join: direct id match on PROJ-prefixed keys
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} proj
    ON na.project = proj.id