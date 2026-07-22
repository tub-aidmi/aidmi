{{ config(materialized='table') }}

WITH asset_projekt_map AS (
    SELECT
        ma.asset_kennung,
        ma.asset_name,
        ma.serien_nummer,
        ma.garantieende,
        ma.projekt_kennung,
        ma.kunden_kennung,
        mp.projekt_kennung AS projekt_kennung_join,
        mp.kunden_kennung AS projekt_kunden_kennung
    FROM
        {{ source('fixture_master_v2_src', 'master_assets') }} ma
    LEFT JOIN
        {{ source('fixture_master_v2_src', 'master_projekte') }} mp
        ON ma.projekt_kennung = mp.projekt_kennung
),

account_uuid_map AS (
    SELECT
        mk.kundennummer,
        gen_random_uuid()::text AS account_id
    FROM
        {{ source('fixture_master_v2_src', 'master_kunden') }} mk
),

project_uuid_map AS (
    SELECT
        mp.projekt_kennung,
        gen_random_uuid()::text AS project_id
    FROM
        {{ source('fixture_master_v2_src', 'master_projekte') }} mp
)

SELECT
    gen_random_uuid()::text AS "Id",
    COALESCE(TRIM(apm.asset_name), 'Unknown Asset') AS "Name",
    TRIM(apm.serien_nummer) AS "Serial_Number__c",
    CASE
        WHEN TRIM(apm.garantieende) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(apm.garantieende)
        WHEN TRIM(apm.garantieende) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(apm.garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(apm.garantieende) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(apm.garantieende), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    aum.account_id AS "Account__c",
    pum.project_id AS "Project__c",
    TRIM(apm.asset_kennung) AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    asset_projekt_map apm
LEFT JOIN
    account_uuid_map aum ON apm.projekt_kunden_kennung = aum.kundennummer
LEFT JOIN
    project_uuid_map pum ON apm.projekt_kennung = pum.projekt_kennung