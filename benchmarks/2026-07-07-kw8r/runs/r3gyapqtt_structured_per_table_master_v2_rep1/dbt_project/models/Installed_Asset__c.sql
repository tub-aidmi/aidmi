{{ config(materialized='table') }}

WITH asset_data AS (
    SELECT
        ma.asset_kennung,
        ma.asset_name,
        ma.serien_nummer,
        ma.garantieende,
        ma.kunden_kennung,
        ma.projekt_kennung,
        mk.kundennummer AS account_id,
        mp.projekt_kennung AS project_id
    FROM {{ source('fixture_master_v2_src', 'master_assets') }} ma
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
        ON ma.kunden_kennung = mk.kundennummer
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} mp
        ON ma.projekt_kennung = mp.projekt_kennung
)

SELECT
    asset_kennung AS "Id",
    asset_name AS "Name",
    serien_nummer AS "Serial_Number__c",
    CASE
        WHEN garantieende IS NULL OR TRIM(garantieende) = 'N/A' THEN NULL
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    account_id AS "Account__c",
    project_id AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM asset_data