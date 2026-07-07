-- dbt model for Installed_Asset__c

{{ config(materialized='table') }}

SELECT
    asset_id AS "Id",
    bezeichnung AS "Name",
    seriennr AS "Serial_Number__c",
    TO_CHAR(CAST(garantie_bis AS DATE), 'YYYY-MM-DD') AS "Warranty_End_Date__c",
    kd_ref AS "Account__c", -- Assuming 'kd_ref' is the kunden_nr from the Account table
    projekt_ref AS "Project__c", -- Assuming 'projekt_ref' is the proj_id from the Project__c table
    asset_id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
