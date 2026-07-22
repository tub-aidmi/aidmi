-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolve

{{ config(materialized='table') }}

SELECT
    CAST('AST_' || ma.asset_kennung AS TEXT) AS "Id",
    ma.asset_name AS "Name",
    ma.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN ma.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    CAST('ACC_' || ma.kunden_kennung AS TEXT) AS "Account__c",
    CAST('PRJ_' || ma.projekt_kennung AS TEXT) AS "Project__c",
    ma.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS ma