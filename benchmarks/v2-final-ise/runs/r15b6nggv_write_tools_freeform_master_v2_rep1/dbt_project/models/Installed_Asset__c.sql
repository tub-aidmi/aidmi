{{ config(materialized='table') }}

WITH asset_data AS (
    SELECT
        asset_kennung,
        asset_name,
        serien_nummer,
        garantieende,
        kunden_kennung,
        projekt_kennung
    FROM {{ source('fixture_master_v2_src', 'master_assets') }}
),

account_mapping AS (
    SELECT
        kundennummer AS "AccountId",
        kundennummer AS "Legacy_Customer_ID__c"
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

project_mapping AS (
    SELECT
        projekt_kennung AS "ProjectId",
        projekt_kennung AS "Legacy_Project_ID__c"
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)

SELECT
    a.asset_kennung AS "Id",
    a.asset_name AS "Name",
    a.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantieende
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN a.garantieende = 'N/A' THEN NULL
        WHEN a.garantieende IS NULL THEN NULL
        ELSE NULL
    END AS "Warranty_End_Date__c",
    am."AccountId" AS "Account__c",
    pm."ProjectId" AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM asset_data a
LEFT JOIN account_mapping am ON a.kunden_kennung = am."Legacy_Customer_ID__c"
LEFT JOIN project_mapping pm ON a.projekt_kennung = pm."Legacy_Project_ID__c"
