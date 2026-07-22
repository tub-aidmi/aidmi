{{ config(materialized='table') }}

SELECT
    TRIM(asset_id) AS "Id",
    TRIM(COALESCE(bezeichnung, 'N/A')) AS "Name",
    TRIM(seriennr) AS "Serial_Number__c",
    COALESCE(
        TO_CHAR(TO_DATE(garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        NULL
    ) AS "Warranty_End_Date__c",
    TRIM(kd_ref) AS "Account__c",
    TRIM(projekt_ref) AS "Project__c",
    TRIM(asset_id) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
