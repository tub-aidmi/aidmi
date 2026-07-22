{{ config(materialized='table') }}

WITH cleaned_assets AS (
    SELECT
        asset_kennung,
        asset_name,
        serien_nummer,
        garantieende,
        kunden_kennung,
        projekt_kennung,
        -- Defaulting CreatedDate and LastModifiedDate as source doesn't provide
        CAST(CURRENT_TIMESTAMP AS TEXT) AS created_date,
        CAST(CURRENT_TIMESTAMP AS TEXT) AS last_modified_date
    FROM
        {{ source('fixture_master_v2_src', 'master_assets') }}
)
SELECT
    MD5(asset_kennung) AS "Id",
    COALESCE(asset_name, 'Unknown Asset') AS "Name", -- Name is NOT NULL
    serien_nummer AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(garantieende, 'YYYY-MM-DD')
            WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(garantieende, 'DD.MM.YYYY')
            WHEN garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(garantieende, 'MM/DD/YYYY')
            ELSE NULL
        END,
    'YYYY-MM-DD') AS "Warranty_End_Date__c",
    MD5(kunden_kennung) AS "Account__c", -- Account__c is derived from kunden_kennung
    MD5(projekt_kennung) AS "Project__c", -- Project__c is derived from projekt_kennung
    asset_kennung AS "Legacy_Asset_ID__c",
    created_date AS "CreatedDate",
    last_modified_date AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_assets
