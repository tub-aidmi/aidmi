
{{ config(materialized='table') }}

SELECT
    TRIM(assets.asset_id) AS "Id",
    COALESCE(TRIM(assets.bezeichnung), 'Unknown Asset') AS "Name",
    TRIM(assets.seriennr) AS "Serial_Number__c",
    TRIM(assets.garantie_bis) AS "Warranty_End_Date__c",
    TRIM(assets.kd_ref) AS "Account__c",
    TRIM(assets.projekt_ref) AS "Project__c",
    TRIM(assets.asset_id) AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'assets') }} AS assets
