-- noinspection SqlNoDataSourceInspection
-- noinspection SqlResolve
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

source_customers AS (
    SELECT
        kundennummer,
        unternehmensname
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

source_projects AS (
    SELECT
        projekt_kennung,
        projektname
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)

SELECT
    -- Id
    CAST(MD5(COALESCE(sa.asset_kennung, 'N/A')) AS TEXT) AS "Id",

    -- Name
    COALESCE(sa.asset_name, sa.asset_kennung) AS "Name",

    -- Serial_Number__c
    sa.serien_nummer AS "Serial_Number__c",

    -- Warranty_End_Date__c
    CASE
        WHEN sa.garantieende ~ '^''\d{2}\/\d{2}\/\d{4}''' THEN TO_CHAR(TO_DATE(sa.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN sa.garantieende ~ '^''\d{8}''' THEN TO_CHAR(TO_DATE(sa.garantieende, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN sa.garantieende ~ '^''\d{2}\.\d{2}\.\d{4}''' THEN TO_CHAR(TO_DATE(sa.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c (Salesforce Account ID derived from customer number)
    CAST(MD5(COALESCE(sc.kundennummer, 'N/A')) AS TEXT) AS "Account__c",

    -- Project__c (Salesforce Project ID derived from project number)
    CAST(MD5(COALESCE(sp.projekt_kennung, 'N/A')) AS TEXT) AS "Project__c",

    -- Legacy_Asset_ID__c
    sa.asset_kennung AS "Legacy_Asset_ID__c",

    -- CreatedDate
    NULL AS "CreatedDate",

    -- LastModifiedDate
    NULL AS "LastModifiedDate",

    -- IsDeleted
    0 AS "IsDeleted"
FROM
    source_assets AS sa
LEFT JOIN
    source_customers AS sc ON sa.kunden_kennung = sc.kundennummer
LEFT JOIN
    source_projects AS sp ON sa.projekt_kennung = sp.projekt_kennung