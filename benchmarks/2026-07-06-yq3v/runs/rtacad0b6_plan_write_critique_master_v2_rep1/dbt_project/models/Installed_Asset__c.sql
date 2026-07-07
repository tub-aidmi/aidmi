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

accounts_cte AS (
    SELECT
        kundennummer AS "Legacy_Customer_ID__c",
        MD5(kundennummer) AS "Id"
    FROM {{ source('fixture_master_v2_src', 'master_kunden') }}
),

projects_cte AS (
    SELECT
        projekt_kennung AS "Legacy_Project_ID__c",
        MD5(projekt_kennung) AS "Id"
    FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
)

SELECT
    MD5(sa.asset_kennung) AS "Id",
    COALESCE(sa.asset_name, 'Asset ' || sa.asset_kennung) AS "Name",
    sa.serien_nummer AS "Serial_Number__c",
    CASE
        WHEN sa.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(sa.garantieende, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN sa.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(sa.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN sa.garantieende ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(sa.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acc."Id" AS "Account__c",
    proj."Id" AS "Project__c",
    sa.asset_kennung AS "Legacy_Asset_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    source_assets sa
LEFT JOIN
    accounts_cte acc ON sa.kunden_kennung = acc."Legacy_Customer_ID__c"
LEFT JOIN
    projects_cte proj ON sa.projekt_kennung = proj."Legacy_Project_ID__c"
