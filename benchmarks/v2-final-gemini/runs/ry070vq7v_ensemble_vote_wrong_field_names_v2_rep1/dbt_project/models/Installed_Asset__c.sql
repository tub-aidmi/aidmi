{{ config(materialized='table') }}

SELECT
    MD5(ast.asset_id) AS "Id",
    COALESCE(ast.bezeichnung, 'Unknown Asset') AS "Name",
    ast.seriennr AS "Serial_Number__c",
    TO_CHAR(
        CASE
            WHEN ast.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN ast.garantie_bis::DATE
            WHEN ast.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(ast.garantie_bis, 'DD.MM.YYYY')
            WHEN ast.garantie_bis ~ '^\d{2}\/\d{2}\/\d{4}$' THEN TO_DATE(ast.garantie_bis, 'MM/DD/YYYY')
            ELSE NULL
        END,
        'YYYY-MM-DD'
    ) AS "Warranty_End_Date__c",
    MD5(knd.kunden_nr) AS "Account__c",
    MD5(prj.proj_id) AS "Project__c",
    ast.asset_id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS ast
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS knd
    ON ast.kd_ref = knd.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS prj
    ON ast.projekt_ref = prj.proj_id
;