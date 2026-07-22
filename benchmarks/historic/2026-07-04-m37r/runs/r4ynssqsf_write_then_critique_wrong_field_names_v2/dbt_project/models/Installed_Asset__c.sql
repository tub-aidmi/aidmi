-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    assets.asset_id AS "Id",
    COALESCE(assets.bezeichnung, 'Unknown Asset') AS "Name",
    assets.seriennr AS "Serial_Number__c",
    CASE
        WHEN assets.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(assets.garantie_bis AS DATE), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    assets.kd_ref AS "Account__c",
    assets.projekt_ref AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets