{{ config(materialized='table') }}

SELECT
    assets.asset_id AS "Id",
    COALESCE(TRIM(assets.bezeichnung), 'Unnamed Asset ' || assets.asset_id) AS "Name",
    TRIM(assets.seriennr) AS "Serial_Number__c",
    CASE
        WHEN assets.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN assets.garantie_bis::DATE
        WHEN assets.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(assets.garantie_bis, 'DD.MM.YYYY')
        WHEN assets.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(assets.garantie_bis, 'MM/DD/YYYY')
        ELSE NULL
    END::TEXT AS "Warranty_End_Date__c",
    TRIM(assets.kd_ref) AS "Account__c",
    TRIM(assets.projekt_ref) AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
