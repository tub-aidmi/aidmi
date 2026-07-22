{{ config(materialized='table') }}

WITH asset_data AS (
    SELECT
        a.id,
        a.name,
        a.serial,
        a.warranty,
        a.client,
        a.project
    FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
),
account_normalized AS (
    SELECT
        id AS account_id,
        name AS account_name,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(name, '[^a-zA-Z0-9 ]', '', 'g'), '\s+', ' ', 'g')) AS normalized_name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
),
asset_with_account AS (
    SELECT
        ad.id,
        ad.name,
        ad.serial,
        ad.warranty,
        ad.client,
        ad.project,
        COALESCE(
            -- Direct match if client is an ACC-* ID
            (SELECT an.account_id FROM account_normalized an WHERE an.account_id = ad.client),
            -- Fallback: match client to account.name (normalized)
            (SELECT an.account_id 
             FROM account_normalized an 
             WHERE LOWER(REGEXP_REPLACE(REGEXP_REPLACE(ad.client, '[^a-zA-Z0-9 ]', '', 'g'), '\s+', ' ', 'g')) = an.normalized_name 
             LIMIT 1)
        ) AS account_id
    FROM asset_data ad
)

SELECT
    id AS "Id",
    COALESCE(NULLIF(TRIM(name), ''), 'Unknown') AS "Name",
    serial AS "Serial_Number__c",
    CASE
        WHEN warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN warranty
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account_id AS "Account__c",
    project AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM asset_with_account
