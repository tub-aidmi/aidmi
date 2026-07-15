{{ config(materialized='table') }}

SELECT
    'a1Y' || LPAD(REGEXP_REPLACE(asset_id, '\D', '', 'g'), 12, '0') AS "Id",
    bezeichnung AS "Name",
    seriennr AS "Serial_Number__c",
    garantie_bis AS "Warranty_End_Date__c",
    '001' || LPAD(REGEXP_REPLACE(kd_ref, '\D', '', 'g'), 12, '0') AS "Account__c",
    'a1X' || LPAD(REGEXP_REPLACE(projekt_ref, '\D', '', 'g'), 12, '0') AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
