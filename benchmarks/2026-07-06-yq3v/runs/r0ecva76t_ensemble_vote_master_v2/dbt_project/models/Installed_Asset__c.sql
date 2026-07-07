{{ config(materialized='table') }}

SELECT
    ma.asset_kennung AS "Id",
    COALESCE(ma.asset_name, ma.asset_kennung) AS "Name",
    ma.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN ma.garantieende IS NOT NULL AND ma.garantieende ~ '^\d{4}-\d{2}-\d{2}$'
        THEN TO_CHAR(TO_DATE(ma.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN ma.garantieende IS NOT NULL AND ma.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$'
        THEN TO_CHAR(TO_DATE(ma.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    ('ACC-' || ma.kunden_kennung) AS "Account__c",
    ('PROJ-' || ma.projekt_kennung) AS "Project__c",
    ma.asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS ma
