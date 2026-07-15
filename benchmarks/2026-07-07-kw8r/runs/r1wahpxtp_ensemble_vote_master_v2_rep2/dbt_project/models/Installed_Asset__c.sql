{{ config(materialized='table') }}

SELECT
    "asset_kennung" AS "Id",
    COALESCE(TRIM("asset_name"), 'Unknown Asset') AS "Name",
    "serien_nummer" AS "Serial_Number__c",
    CASE
        WHEN "garantieende" IS NULL OR "garantieende" = 'N/A' THEN NULL
        WHEN "garantieende" ~ '^\d{4}-\d{2}-\d{2}$' THEN "garantieende"
        WHEN "garantieende" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE("garantieende", 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN "garantieende" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE("garantieende", 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN "garantieende" ~ '^\d{8}$' THEN TO_CHAR(TO_DATE("garantieende", 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    "kunden_kennung" AS "Account__c",
    "projekt_kennung" AS "Project__c",
    "asset_kennung" AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }}