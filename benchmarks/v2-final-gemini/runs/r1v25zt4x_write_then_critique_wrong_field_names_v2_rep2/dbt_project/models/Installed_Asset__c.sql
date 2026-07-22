{{ config(materialized='table') }}

SELECT
    assets.asset_id AS "Id",
    COALESCE(assets.bezeichnung, assets.asset_id) AS "Name",
    assets.seriennr AS "Serial_Number__c",
    CASE
        WHEN assets.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(assets.kd_ref) AS "Account__c",
    MD5(assets.projekt_ref) AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets