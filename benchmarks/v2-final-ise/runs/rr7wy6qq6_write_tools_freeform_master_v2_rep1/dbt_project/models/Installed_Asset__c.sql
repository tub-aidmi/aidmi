{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        a.asset_kennung,
        a.asset_name,
        a.serien_nummer,
        a.garantieende,
        a.kunden_kennung,
        a.projekt_kennung,
        k.kundennummer AS account_kundennummer,
        p.projekt_kennung AS project_projekt_kennung
    FROM {{ source('fixture_master_v2_src', 'master_assets') }} a
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} k
        ON a.kunden_kennung = k.kundennummer
    LEFT JOIN {{ source('fixture_master_v2_src', 'master_projekte') }} p
        ON a.projekt_kennung = p.projekt_kennung
),

normalized AS (
    SELECT
        asset_kennung,
        INITCAP(TRIM(asset_name)) AS name,
        TRIM(serien_nummer) AS serial_number,
        TRIM(garantieende) AS warranty_end_date,
        account_kundennummer,
        project_projekt_kennung
    FROM source_data
),

parsed_date AS (
    SELECT
        asset_kennung,
        name,
        serial_number,
        CASE
            WHEN warranty_end_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN warranty_end_date
            WHEN warranty_end_date ~ '^[0-9]{2}\.[0-9]{2}\.[0-9]{4}$' THEN
                TO_CHAR(TO_DATE(warranty_end_date, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN warranty_end_date ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN
                TO_CHAR(TO_DATE(warranty_end_date, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN warranty_end_date ~ '^[0-9]{8}$' THEN
                TO_CHAR(TO_DATE(warranty_end_date, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END AS warranty_end_iso,
        account_kundennummer,
        project_projekt_kennung
    FROM normalized
)

SELECT
    MD5(asset_kennung || '_ASSET') AS "Id",
    name AS "Name",
    serial_number AS "Serial_Number__c",
    warranty_end_iso AS "Warranty_End_Date__c",
    CASE
        WHEN account_kundennummer IS NOT NULL
        THEN MD5(account_kundennummer || '_ACCOUNT')
        ELSE NULL
    END AS "Account__c",
    CASE
        WHEN project_projekt_kennung IS NOT NULL
        THEN MD5(project_projekt_kennung || '_PROJECT')
        ELSE NULL
    END AS "Project__c",
    asset_kennung AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM parsed_date
