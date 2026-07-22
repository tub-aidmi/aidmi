{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    a.bezeichnung AS "Name",
    a.seriennr AS "Serial_Number__c",
    TO_CHAR(CAST(a.garantie_bis AS DATE), 'YYYY-MM-DD') AS "Warranty_End_Date__c",
    a.kd_ref AS "Account__c", -- This is kunden_nr from kunden, which is the Account.Id
    a.projekt_ref AS "Project__c", -- This is proj_id from proj, which is the Project__c.Id
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS a
