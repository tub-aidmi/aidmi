{{ config(materialized='table') }}

SELECT
    '003' || SUBSTRING(MD5(a.asset_id), 1, 15) AS "Id",
    a.bezeichnung AS "Name",
    a.seriennr AS "Serial_Number__c",
    a.garantie_bis AS "Warranty_End_Date__c",
    '001' || SUBSTRING(MD5(a.kd_ref), 1, 15) AS "Account__c",
    '002' || SUBSTRING(MD5(a.projekt_ref), 1, 15) AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a