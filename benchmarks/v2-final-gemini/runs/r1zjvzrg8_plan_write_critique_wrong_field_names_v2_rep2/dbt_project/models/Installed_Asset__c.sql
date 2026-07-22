{{ config(materialized='table') }}

SELECT
    asset.asset_id AS "Id",
    COALESCE(TRIM(INITCAP(asset.bezeichnung)), 'Unknown Asset') AS "Name",
    asset.seriennr AS "Serial_Number__c",
    CASE
        WHEN asset.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN asset.garantie_bis
        ELSE NULL
    END AS "Warranty_End_Date__c",
    asset.kd_ref AS "Account__c",
    asset.projekt_ref AS "Project__c",
    asset.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS asset
