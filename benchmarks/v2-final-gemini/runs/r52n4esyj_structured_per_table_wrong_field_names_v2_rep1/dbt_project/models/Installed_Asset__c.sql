-- noinspection SqlNoDataSourceInspectionForFile
{{ config(materialized='table') }}

SELECT
    CAST(assets.asset_id AS TEXT) AS "Id",
    COALESCE(assets.bezeichnung, assets.asset_id) AS "Name",
    CAST(assets.seriennr AS TEXT) AS "Serial_Number__c",
    CASE
        WHEN assets.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN assets.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN assets.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CAST(assets.kd_ref AS TEXT) AS "Account__c",
    CAST(assets.projekt_ref AS TEXT) AS "Project__c",
    CAST(assets.asset_id AS TEXT) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets