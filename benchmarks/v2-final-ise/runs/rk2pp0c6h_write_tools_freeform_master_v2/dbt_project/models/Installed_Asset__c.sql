{{ config(materialized='table') }}

WITH asset_data AS (
    SELECT
        a.asset_kennung,
        a.asset_name,
        a.serien_nummer,
        a.garantieende,
        a.kunden_kennung,
        a.projekt_kennung,
        c.kundennummer AS account_kundennummer,
        p.projekt_kennung AS project_projekt_kennung
    FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} c 
        ON a.kunden_kennung = c.kundennummer
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p 
        ON a.projekt_kennung = p.projekt_kennung
)

SELECT
    asset_kennung AS "Id",
    asset_name AS "Name",
    serien_nummer AS "Serial_Number__c",
    CASE 
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN garantieende
        WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account_kundennummer AS "Account__c",
    project_projekt_kennung AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM asset_data
