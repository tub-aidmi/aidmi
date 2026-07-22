-- dbt model for Installed_Asset__c

{{ config(materialized='table') }}

WITH
asset_data AS (
    SELECT
        ma.asset_kennung,
        ma.asset_name,
        ma.serien_nummer,
        ma.garantieende,
        ma.kunden_kennung,
        ma.projekt_kennung
    FROM
        {{ source('fixture_master_v2_src', 'master_assets') }} AS ma
),
account_map AS (
    SELECT
        mk.kundennummer,
        MD5(mk.kundennummer) AS account_id -- Salesforce Id for Account
    FROM
        {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
),
project_map AS (
    SELECT
        mp.projekt_kennung,
        MD5(mp.projekt_kennung) AS project_id -- Salesforce Id for Project__c
    FROM
        {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
)
SELECT
    MD5(ad.asset_kennung) AS "Id",
    COALESCE(ad.asset_name, 'Asset ' || ad.asset_kennung) AS "Name",
    ad.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN ad.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(ad.garantieende::DATE, 'YYYY-MM-DD') -- YYYY-MM-DD format
        WHEN ad.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ad.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY format
        WHEN ad.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(ad.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY format
        ELSE NULL
    END AS "Warranty_End_Date__c",
    am.account_id AS "Account__c",
    pm.project_id AS "Project__c",
    ad.asset_kennung AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate", -- No source for CreatedDate
    NULL::TEXT AS "LastModifiedDate", -- No source for LastModifiedDate
    0 AS "IsDeleted"
FROM
    asset_data AS ad
LEFT JOIN
    account_map AS am ON ad.kunden_kennung = am.kundennummer
LEFT JOIN
    project_map AS pm ON ad.projekt_kennung = pm.projekt_kennung