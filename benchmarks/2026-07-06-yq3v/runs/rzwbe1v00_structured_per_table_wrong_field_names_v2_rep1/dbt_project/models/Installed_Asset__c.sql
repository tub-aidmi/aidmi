-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlDialectInspectionForFile
{{ config(materialized='table') }}

SELECT
    assets.asset_id AS "Id",
    assets.bezeichnung AS "Name",
    assets.seriennr AS "Serial_Number__c",
    assets.garantie_bis AS "Warranty_End_Date__c",
    kunden.kunden_nr AS "Account__c",
    proj.proj_id AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON assets.kd_ref = kunden.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS proj
    ON assets.projekt_ref = proj.proj_id