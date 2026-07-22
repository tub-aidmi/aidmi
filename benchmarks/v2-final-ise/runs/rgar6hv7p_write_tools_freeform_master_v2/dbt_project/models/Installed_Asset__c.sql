{{ config(materialized='table') }}

WITH assets AS (
    SELECT
        asset_kennung,
        asset_name,
        serien_nummer,
        garantieende,
        kunden_kennung,
        projekt_kennung
    FROM {{ source('fixture_master_v2_src', 'master_assets') }}
),

kunden AS (
    SELECT
        kundennummer,
        '001' || ENCODE(DIGEST(kundennummer, 'md5'), 'hex') AS account_id
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

projekte AS (
    SELECT
        projekt_kennung,
        '701' || ENCODE(DIGEST(projekt_kennung, 'md5'), 'hex') AS project_id
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
),

asset_mapping AS (
    SELECT
        '021' || ENCODE(DIGEST(a.asset_kennung, 'md5'), 'hex') AS Id,
        INITCAP(TRIM(a.asset_name)) AS Name,
        NULLIF(TRIM(a.serien_nummer), '') AS "Serial_Number__c",
        CASE 
            WHEN a.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantieende
            WHEN a.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN 
                TO_CHAR(TO_DATE(a.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN a.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN 
                TO_CHAR(TO_DATE(a.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN a.garantieende ~ '^\d{8}$' THEN 
                TO_CHAR(TO_DATE(a.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS "Warranty_End_Date__c",
        kd.account_id AS Account__c,
        p.project_id AS Project__c,
        a.asset_kennung AS Legacy_Asset_ID__c,
        TO_CHAR(NOW(), 'YYYY-MM-DD') AS CreatedDate,
        TO_CHAR(NOW(), 'YYYY-MM-DD') AS LastModifiedDate,
        0 AS IsDeleted
    FROM assets a
    LEFT JOIN kunden kd ON a.kunden_kennung = kd.kundennummer
    LEFT JOIN projekte p ON a.projekt_kennung = p.projekt_kennung
)

SELECT
    Id,
    Name,
    "Serial_Number__c",
    "Warranty_End_Date__c",
    Account__c,
    Project__c,
    Legacy_Asset_ID__c,
    CreatedDate,
    LastModifiedDate,
    IsDeleted
FROM asset_mapping
