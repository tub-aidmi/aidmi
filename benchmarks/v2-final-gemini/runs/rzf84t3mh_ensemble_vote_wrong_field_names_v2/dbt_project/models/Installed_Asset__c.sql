{{ config(materialized='table') }}

WITH source_assets AS (
    SELECT
        asset_id,
        bezeichnung,
        seriennr,
        garantie_bis,
        kd_ref,
        projekt_ref
    FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
),
source_kunden AS (
    SELECT
        kunden_nr,
        erp_nummer
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),
source_proj AS (
    SELECT
        proj_id,
        name
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
)

SELECT
    MD5(sa.asset_id) AS "Id",
    sa.bezeichnung AS "Name",
    sa.seriennr AS "Serial_Number__c",
    CASE
        WHEN sa.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(sa.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN sa.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(sa.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN sa.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(sa.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    MD5(sk.kunden_nr) AS "Account__c",
    MD5(sp.proj_id) AS "Project__c",
    sa.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_assets AS sa
LEFT JOIN source_kunden AS sk
    ON sa.kd_ref = sk.kunden_nr
LEFT JOIN source_proj AS sp
    ON sa.projekt_ref = sp.proj_id
