-- models/Installed_Asset__c.sql

{{ config(materialized='table') }}

WITH asset_with_account_id AS (
    SELECT
        a.id,
        a.name,
        a.serial,
        a.warranty,
        a.project,
        CASE
            WHEN a.client LIKE 'ACC-%' THEN a.client -- client is already an Account ID
            ELSE acc.id -- client is an Account Name, join to get Account ID
        END AS account_id_mapped
    FROM
        {{ source('fixture_missing_relations_v2_src', 'asset') }} AS a
    LEFT JOIN
        {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc
    ON
        a.client = acc.name
)
SELECT
    id AS "Id",
    COALESCE(name, 'Unknown Asset') AS "Name",
    serial AS "Serial_Number__c",
    warranty AS "Warranty_End_Date__c",
    account_id_mapped AS "Account__c",
    project AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    asset_with_account_id
