-- models/Installed_Asset__c.sql

{{ config(materialized='table') }}

SELECT
    TRIM(asset_kennung) AS "Id",
    COALESCE(TRIM(asset_name), 'Unknown Asset') AS "Name",
    TRIM(serien_nummer) AS "Serial_Number__c",
    COALESCE(
        TO_CHAR(CASE WHEN TRIM(garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(garantieende), 'YYYY-MM-DD') ELSE NULL END, 'YYYY-MM-DD'),
        TO_CHAR(CASE WHEN TRIM(garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(garantieende), 'DD.MM.YYYY') ELSE NULL END, 'YYYY-MM-DD'),
        TO_CHAR(CASE WHEN TRIM(garantieende) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(garantieende), 'MM/DD/YYYY') ELSE NULL END, 'YYYY-MM-DD'),
        NULL -- Allow NULL as target column is not NOT NULL
    ) AS "Warranty_End_Date__c",
    TRIM(kunden_kennung) AS "Account__c",
    TRIM(projekt_kennung) AS "Project__c",
    TRIM(asset_kennung) AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }}
