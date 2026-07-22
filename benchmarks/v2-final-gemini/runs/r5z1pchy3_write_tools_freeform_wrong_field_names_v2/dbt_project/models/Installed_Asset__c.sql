-- models/Installed_Asset__c.sql

{{ config(materialized='table') }}

SELECT
    asset_id AS "Id",
    bezeichnung AS "Name",
    seriennr AS "Serial_Number__c",
    TO_CHAR(CAST(garantie_bis AS DATE), 'YYYY-MM-DD') AS "Warranty_End_Date__c",
    kd_ref AS "Account__c",
    projekt_ref AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
