{{ config(materialized='table') }}

SELECT
    asset_kennung AS "Id",
    COALESCE(TRIM(asset_name), 'Unknown') AS "Name",
    serien_nummer AS "Serial_Number__c",
    CASE
        WHEN garantieende IS NULL OR TRIM(garantieende) = '' THEN NULL
        WHEN TRIM(UPPER(garantieende)) = 'N/A' THEN NULL
        WHEN TRIM(garantieende) = '0000-00-00' THEN NULL
        WHEN garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(garantieende), 'YYYY-MM-DD')::TEXT
        WHEN garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(garantieende), 'DD.MM.YYYY')::TEXT
        WHEN garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(garantieende), 'MM/DD/YYYY')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",
    kunden_kennung AS "Account__c",
    projekt_kennung AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_assets') }}