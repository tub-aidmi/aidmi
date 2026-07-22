{{ config(materialized='table') }}

SELECT
    ma.asset_kennung AS Id,
    ma.asset_name AS Name,
    ma.serien_nummer AS Serial_Number__c,
    CASE
        WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN ma.garantieende
        WHEN ma.garantieende ~ '^\d{8}$' THEN
            SUBSTRING(ma.garantieende, 1, 4) || '-' || SUBSTRING(ma.garantieende, 5, 2) || '-' || SUBSTRING(ma.garantieende, 7, 2)
        WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
            SUBSTRING(ma.garantieende, 7, 4) || '-' || SUBSTRING(ma.garantieende, 4, 2) || '-' || SUBSTRING(ma.garantieende, 1, 2)
        ELSE NULL
    END AS Warranty_End_Date__c,
    COALESCE(ma.kunden_kennung, mk.kundennummer) AS Account__c,
    ma.projekt_kennung AS Project__c,
    ma.asset_kennung AS Legacy_Asset_ID__c,
    NULL::text AS CreatedDate,
    NULL::text AS LastModifiedDate,
    0 AS IsDeleted

FROM {{ source('fixture_master_src', 'master_assets') }} ma
LEFT JOIN {{ source('fixture_master_src', 'master_kunden') }} mk ON ma.kunden_kennung = mk.kundennummer