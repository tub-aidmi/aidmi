{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    COALESCE(a.bezeichnung, a.asset_id) AS "Name",
    a.seriennr AS "Serial_Number__c",
    a.garantie_bis AS "Warranty_End_Date__c",
    a.kd_ref AS "Account__c",
    a.projekt_ref AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS a
