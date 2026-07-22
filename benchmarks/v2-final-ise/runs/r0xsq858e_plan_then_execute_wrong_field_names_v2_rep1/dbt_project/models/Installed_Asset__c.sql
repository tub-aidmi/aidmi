{{ config(materialized='table') }}

WITH asset_data AS (
    SELECT
        a.asset_id,
        TRIM(a.bezeichnung) AS bezeichnung,
        TRIM(a.seriennr) AS seriennr,
        a.garantie_bis,
        a.kd_ref,
        a.projekt_ref,
        k.kunden_nr,
        p.proj_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON a.kd_ref = k.kunden_nr
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p ON a.projekt_ref = p.proj_id
)

SELECT
    gen_random_uuid()::text AS "Id",
    COALESCE(NULLIF(TRIM(bezeichnung), ''), asset_id) AS "Name",
    NULLIF(TRIM(seriennr), '') AS "Serial_Number__c",
    CASE 
        WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN garantie_bis
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    kunden_nr AS "Account__c",
    proj_id AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM asset_data