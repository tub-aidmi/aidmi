{{ config(materialized='table') }}

WITH asset_data AS (
    SELECT
        ma.asset_kennung,
        ma.asset_name,
        ma.serien_nummer,
        ma.garantieende,
        ma.kunden_kennung,
        ma.projekt_kennung
    FROM {{ source('fixture_master_v2_src', 'master_assets') }} ma
),
account_mapping AS (
    SELECT
        mk.kundennummer,
        '001' || SUBSTRING(MD5(mk.kundennummer), 1, 15) AS sf_account_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }} mk
),
project_mapping AS (
    SELECT
        mp.projekt_kennung,
        '701' || SUBSTRING(MD5(mp.projekt_kennung), 1, 15) AS sf_project_id
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }} mp
)
SELECT
    '02i' || SUBSTRING(MD5(ad.asset_kennung), 1, 15) AS "Id",
    COALESCE(NULLIF(TRIM(ad.asset_name), ''), 'Unknown') AS "Name",
    ad.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN ad.garantieende IS NULL OR ad.garantieende = 'N/A' THEN NULL
        WHEN ad.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN ad.garantieende
        WHEN ad.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
            TO_CHAR(TO_DATE(ad.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ad.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN 
            TO_CHAR(TO_DATE(ad.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN ad.garantieende ~ '^\d{8}$' THEN 
            TO_CHAR(TO_DATE(ad.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    am.sf_account_id AS "Account__c",
    pm.sf_project_id AS "Project__c",
    ad.asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM asset_data ad
LEFT JOIN account_mapping am ON ad.kunden_kennung = am.kundennummer
LEFT JOIN project_mapping pm ON ad.projekt_kennung = pm.projekt_kennung