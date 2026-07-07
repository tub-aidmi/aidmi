{{ config(materialized='table') }}

WITH account_ids AS (
    SELECT
        kunden_nr,
        MD5(kunden_nr) AS account_id_uuid
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),
project_ids AS (
    SELECT
        proj_id,
        MD5(proj_id) AS project_id_uuid
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
)
SELECT
    MD5(ast.asset_id) AS "Id",
    COALESCE(ast.bezeichnung, 'Unnamed Asset') AS "Name",
    ast.seriennr AS "Serial_Number__c",
    CASE
        WHEN ast.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(ast.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN ast.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(ast.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN ast.garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(ast.garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN ast.garantie_bis ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(ast.garantie_bis, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acc.account_id_uuid AS "Account__c",
    prj.project_id_uuid AS "Project__c",
    ast.asset_id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS ast
LEFT JOIN
    account_ids AS acc ON ast.kd_ref = acc.kunden_nr
LEFT JOIN
    project_ids AS prj ON ast.projekt_ref = prj.proj_id
