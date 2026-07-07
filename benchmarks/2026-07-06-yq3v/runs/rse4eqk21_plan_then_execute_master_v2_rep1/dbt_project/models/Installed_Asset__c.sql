-- This model transforms data from the master_assets source into the Installed_Asset__c target table.

{{ config(materialized='table') }}

SELECT
    MD5(master_assets.asset_kennung) AS "Id",
    COALESCE(master_assets.asset_name, master_assets.asset_kennung) AS "Name",
    master_assets.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN master_assets.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(master_assets.garantieende AS DATE), 'YYYY-MM-DD')
        WHEN master_assets.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(master_assets.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN master_assets.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(master_assets.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(master_assets.kunden_kennung) AS "Account__c",
    MD5(master_assets.projekt_kennung) AS "Project__c",
    master_assets.asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS master_assets