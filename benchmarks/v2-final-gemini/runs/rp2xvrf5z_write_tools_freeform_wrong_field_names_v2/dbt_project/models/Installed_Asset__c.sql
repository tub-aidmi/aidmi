-- models/Installed_Asset__c.sql

{{ config(materialized='table') }}

SELECT
    assets.asset_id AS "Id",
    COALESCE(assets.bezeichnung, assets.asset_id) AS "Name",
    assets.seriennr AS "Serial_Number__c",
    COALESCE(
        TO_CHAR(NULLIF(TO_DATE(assets.garantie_bis, 'YYYY-MM-DD'), '0001-01-01'), 'YYYY-MM-DD'),
        TO_CHAR(NULLIF(TO_DATE(assets.garantie_bis, 'DD.MM.YYYY'), '0001-01-01'), 'YYYY-MM-DD'),
        TO_CHAR(NULLIF(TO_DATE(assets.garantie_bis, 'MM/DD/YYYY'), '0001-01-01'), 'YYYY-MM-DD'),
        TO_CHAR(NULLIF(TO_DATE(assets.garantie_bis, 'YYYYMMDD'), '0001-01-01'), 'YYYY-MM-DD'),
        NULL
    ) AS "Warranty_End_Date__c",
    assets.kd_ref AS "Account__c",
    assets.projekt_ref AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
