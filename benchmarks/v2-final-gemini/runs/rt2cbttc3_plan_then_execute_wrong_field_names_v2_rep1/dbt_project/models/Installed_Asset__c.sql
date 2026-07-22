-- dbt model for Installed_Asset__c

{{ config(materialized='table') }}

SELECT
    MD5(assets.asset_id) AS "Id",
    COALESCE(TRIM(assets.bezeichnung), 'Unknown Asset') AS "Name",
    TRIM(assets.seriennr) AS "Serial_Number__c",
    CASE
        WHEN assets.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(assets.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(assets.kd_ref) AS "Account__c",
    MD5(assets.projekt_ref) AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets