{{ config(materialized='table') }}

SELECT
    SUBSTRING(MD5(ma.asset_kennung), 1, 18) AS "Id",
    COALESCE(ma.asset_name, 'Unknown Asset ' || ma.asset_kennung) AS "Name",
    ma.serien_nummer AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(ma.garantieende, 'YYYY-MM-DD')
            WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(ma.garantieende, 'DD.MM.YYYY')
            WHEN ma.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(ma.garantieende, 'MM/DD/YYYY')
            ELSE NULL
        END, 'YYYY-MM-DD'
    ) AS "Warranty_End_Date__c",
    SUBSTRING(MD5(mk.kundennummer), 1, 18) AS "Account__c",
    SUBSTRING(MD5(mp.projekt_kennung), 1, 18) AS "Project__c",
    ma.asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS ma
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mk
    ON ma.kunden_kennung = mk.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS mp
    ON ma.projekt_kennung = mp.projekt_kennung