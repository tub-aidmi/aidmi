
{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    COALESCE(TRIM(a.bezeichnung), TRIM(a.asset_id)) AS "Name",
    TRIM(a.seriennr) AS "Serial_Number__c",
    TO_CHAR(CAST(a.garantie_bis AS DATE), 'YYYY-MM-DD') AS "Warranty_End_Date__c",
    TRIM(a.kd_ref) AS "Account__c",
    TRIM(a.projekt_ref) AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'assets') }} AS a
