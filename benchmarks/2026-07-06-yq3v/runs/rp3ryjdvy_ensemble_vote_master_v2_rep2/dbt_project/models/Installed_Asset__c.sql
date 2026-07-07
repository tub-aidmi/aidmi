{{ config(materialized='table') }}

WITH source_assets AS (
    SELECT
        asset_kennung,
        asset_name,
        serien_nummer,
        garantieende,
        kunden_kennung,
        projekt_kennung
    FROM {{ source('fixture_master_v2_src', 'master_assets') }}
),
source_kunden AS (
    SELECT
        kundennummer
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),
source_projekte AS (
    SELECT
        projekt_kennung
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)

SELECT
    MD5(A.asset_kennung) AS "Id",
    A.asset_name AS "Name",
    A.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN A.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(A.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(A.kunden_kennung) AS "Account__c",
    MD5(A.projekt_kennung) AS "Project__c",
    A.asset_kennung AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_assets AS A
LEFT JOIN
    source_kunden AS K ON A.kunden_kennung = K.kundennummer
LEFT JOIN
    source_projekte AS P ON A.projekt_kennung = P.projekt_kennung