-- depends_on: {{ ref('Account') }}
-- depends_on: {{ ref('Project__c') }}

{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    a.bezeichnung AS "Name",
    a.seriennr AS "Serial_Number__c",
    a.garantie_bis AS "Warranty_End_Date__c",
    a.kd_ref AS "Account__c", -- This assumes kd_ref is directly the Salesforce Account Id
    a.projekt_ref AS "Project__c", -- This assumes projekt_ref is directly the Salesforce Project Id
    a.asset_id AS "Legacy_Asset_ID__c",
    NOW()::TEXT AS "CreatedDate",
    NOW()::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS a