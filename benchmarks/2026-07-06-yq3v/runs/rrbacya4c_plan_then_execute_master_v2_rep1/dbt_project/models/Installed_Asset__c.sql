{{ config(materialized='table') }}

WITH source_assets AS (
    SELECT
        asset_kennung,
        asset_name,
        serien_nummer,
        garantieende,
        kunden_kennung,
        projekt_kennung
    FROM
        {{ source('fixture_master_v2_src', 'master_assets') }}
)
SELECT
    gen_random_uuid()::TEXT AS "Id",
    COALESCE(TRIM(sa.asset_name), 'Unknown Asset') AS "Name",
    TRIM(sa.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN TRIM(sa.garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(sa.garantieende), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    NULL::TEXT AS "Account__c", -- Cannot reference Account.Id due to gen_random_uuid() and "no ref" rule
    NULL::TEXT AS "Project__c", -- Cannot reference Project__c.Id due to gen_random_uuid() and "no ref" rule
    TRIM(sa.asset_kennung) AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_assets sa
