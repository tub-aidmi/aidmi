{{ config(materialized='table') }}

SELECT
    'ASSET_' || asset_kennung AS "Id",
    asset_name AS "Name",
    serien_nummer AS "Serial_Number__c",
    CASE 
        WHEN garantieende ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    'ACC_' || kunden_kennung AS "Account__c",
    CASE 
        WHEN projekt_kennung IS NOT NULL THEN 'PROJ_' || projekt_kennung
        ELSE NULL
    END AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }}
