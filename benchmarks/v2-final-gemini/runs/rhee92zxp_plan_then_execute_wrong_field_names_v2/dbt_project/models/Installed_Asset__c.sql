-- depends_on: {{ ref('Account') }} , {{ ref('Project__c') }}

{{ config(materialized='table') }}

WITH assets AS (
    SELECT
        asset_id,
        bezeichnung,
        seriennr,
        garantie_bis,
        kd_ref,
        projekt_ref
    FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
),

kunden AS (
    SELECT
        kunden_nr,
        firmenname
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),

proj AS (
    SELECT
        proj_id,
        name
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
)

SELECT
    SHA256(assets.asset_id::bytea)::text AS "Id",
    TRIM(assets.bezeichnung) AS "Name",
    assets.seriennr AS "Serial_Number__c",
    COALESCE(
        TO_CHAR(TO_DATE(assets.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(assets.garantie_bis, 'YYYYMMDD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(assets.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        NULL
    ) AS "Warranty_End_Date__c",
    SHA256(kunden.kunden_nr::bytea)::text AS "Account__c",
    SHA256(proj.proj_id::bytea)::text AS "Project__c",
    assets.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM assets
LEFT JOIN kunden ON assets.kd_ref = kunden.kunden_nr
LEFT JOIN proj ON assets.projekt_ref = proj.proj_id