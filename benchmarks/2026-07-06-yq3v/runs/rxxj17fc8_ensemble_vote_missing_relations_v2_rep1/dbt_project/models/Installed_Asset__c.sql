-- This dbt model transforms raw asset data into the Installed_Asset__c target schema.

{{ config(materialized='table') }}

WITH account_lookup AS (
    SELECT
        id,
        name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
),
asset_with_resolved_account AS (
    SELECT
        asset.id,
        asset.name,
        asset.serial,
        asset.warranty,
        asset.client,
        asset.project,
        COALESCE(
            acc_by_id.id,       -- Prioritize matching asset.client to account.id
            acc_by_name.id      -- Fallback to matching asset.client to account.name
        ) AS resolved_account_id
    FROM
        {{ source('fixture_missing_relations_v2_src', 'asset') }} AS asset
    LEFT JOIN
        account_lookup AS acc_by_id
        ON asset.client = acc_by_id.id
    LEFT JOIN
        account_lookup AS acc_by_name
        ON asset.client = acc_by_name.name
)
SELECT
    asset_with_resolved_account.id AS "Id",
    COALESCE(asset_with_resolved_account.name, 'Unknown Asset Name') AS "Name",
    asset_with_resolved_account.serial AS "Serial_Number__c",
    CASE
        WHEN asset_with_resolved_account.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(asset_with_resolved_account.warranty::DATE, 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    asset_with_resolved_account.resolved_account_id AS "Account__c",
    project.id AS "Project__c",
    asset_with_resolved_account.id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    asset_with_resolved_account
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS project
    ON asset_with_resolved_account.project = project.id