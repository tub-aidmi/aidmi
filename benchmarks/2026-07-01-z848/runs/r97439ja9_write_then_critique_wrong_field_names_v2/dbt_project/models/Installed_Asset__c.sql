
{{ config(materialized='table') }}

SELECT
    asset.asset_id AS "Id",
    COALESCE(asset.bezeichnung, 'Unknown Asset Name') AS "Name",
    asset.seriennr AS "Serial_Number__c",
    CASE
        WHEN asset.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(asset.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    asset.kd_ref AS "Account__c",
    asset.projekt_ref AS "Project__c",
    asset.asset_id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS asset
