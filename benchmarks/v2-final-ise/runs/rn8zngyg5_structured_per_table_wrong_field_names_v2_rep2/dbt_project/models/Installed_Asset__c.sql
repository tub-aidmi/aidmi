{{ config(materialized='table') }}

SELECT
    CAST(asset_id AS TEXT) AS "Id",
    INITCAP(TRY_CAST(bezeichnung AS TEXT)) AS "Name",
    TRY_CAST(seriennr AS TEXT) AS "Serial_Number__c",
    TRY_CAST(garantie_bis AS TEXT) AS "Warranty_End_Date__c",
    TRY_CAST(kd_ref AS TEXT) AS "Account__c",
    TRY_CAST(projekt_ref AS TEXT) AS "Project__c",
    CAST(asset_id AS TEXT) AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}