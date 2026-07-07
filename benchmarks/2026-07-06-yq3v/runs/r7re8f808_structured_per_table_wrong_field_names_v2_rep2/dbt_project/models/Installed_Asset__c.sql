-- depends_on: fixture_wrong_field_names_v2_src.assets
{{ config(materialized='table') }}

SELECT
    TRIM(asset.asset_id) AS "Id",
    TRIM(asset.bezeichnung) AS "Name",
    TRIM(asset.seriennr) AS "Serial_Number__c",
    CASE
        WHEN asset.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(asset.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN asset.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(asset.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN asset.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(asset.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(asset.kd_ref) AS "Account__c",
    TRIM(asset.projekt_ref) AS "Project__c",
    TRIM(asset.asset_id) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS asset