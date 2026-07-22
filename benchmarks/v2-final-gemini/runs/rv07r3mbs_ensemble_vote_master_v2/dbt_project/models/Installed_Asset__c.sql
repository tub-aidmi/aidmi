{{ config(materialized='table') }}

SELECT
    a.asset_kennung AS "Id",
    COALESCE(a.asset_name, 'Unknown Asset') AS "Name",
    a.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantieende -- YYYY-MM-DD
        WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantieende ~ '^\d{4}\d{2}\d{2}$' THEN TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    k.kundennummer AS "Account__c",
    p.projekt_kennung AS "Project__c",
    a.asset_kennung AS "Legacy_Asset_ID__c",
    NOW() AS "CreatedDate",
    NOW() AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS a
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
    ON a.kunden_kennung = k.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
    ON a.projekt_kennung = p.projekt_kennung
WHERE a.asset_kennung IS NOT NULL
