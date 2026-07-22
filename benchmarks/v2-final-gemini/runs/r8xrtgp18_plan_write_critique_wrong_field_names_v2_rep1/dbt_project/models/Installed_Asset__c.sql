{{ config(materialized='table') }}

WITH source_assets AS (
    SELECT
        TRIM(asset_id) AS asset_id,
        TRIM(bezeichnung) AS bezeichnung,
        TRIM(seriennr) AS seriennr,
        TRIM(garantie_bis) AS garantie_bis,
        TRIM(kd_ref) AS kd_ref,
        TRIM(projekt_ref) AS projekt_ref
    FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
),

source_kunden AS (
    SELECT
        TRIM(kunden_nr) AS kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),

source_proj AS (
    SELECT
        TRIM(proj_id) AS proj_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
)

SELECT
    a.asset_id AS "Id",
    COALESCE(a.bezeichnung, a.asset_id) AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    k.kunden_nr AS "Account__c",
    p.proj_id AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_assets AS a
LEFT JOIN source_kunden AS k ON a.kd_ref = k.kunden_nr
LEFT JOIN source_proj AS p ON a.projekt_ref = p.proj_id
