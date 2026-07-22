-- depends_on: {{ ref('Account') }}
-- depends_on: {{ ref('Project__c') }}

{{ config(materialized='table') }}

SELECT
    MD5(ma.asset_kennung) AS "Id",
    COALESCE(ma.asset_name, ma.asset_kennung) AS "Name",
    ma.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN ma.garantieende
        WHEN ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ma.garantieende ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(mk.kundennummer) AS "Account__c",
    MD5(mp.projekt_kennung) AS "Project__c",
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