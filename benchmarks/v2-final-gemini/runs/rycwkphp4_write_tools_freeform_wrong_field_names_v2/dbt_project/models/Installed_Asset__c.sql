-- models/Installed_Asset__c.sql
{{ config(materialized='table') }}

SELECT
    asset_id AS "Id",
    COALESCE(bezeichnung, 'Unknown Asset') AS "Name", -- Name is NOT NULL
    seriennr AS "Serial_Number__c",
    garantie_bis AS "Warranty_End_Date__c",
    kd_ref AS "Account__c", -- Maps to kunden.kunden_nr
    projekt_ref AS "Project__c", -- Maps to proj.proj_id
    asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
