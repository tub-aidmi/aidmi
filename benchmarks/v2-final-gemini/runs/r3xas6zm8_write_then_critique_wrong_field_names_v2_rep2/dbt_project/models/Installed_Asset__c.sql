{{ config(materialized='table') }}

SELECT
    assets.asset_id AS "Id",
    COALESCE(assets.bezeichnung, assets.asset_id) AS "Name",
    assets.seriennr AS "Serial_Number__c",
    CASE
        WHEN assets.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN assets.garantie_bis
        WHEN assets.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN assets.garantie_bis ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN assets.garantie_bis ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
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
