{{ config(materialized='table') }}

SELECT
    MD5(ast.asset_id) AS "Id",
    COALESCE(ast.bezeichnung, 'Unknown Asset') AS "Name",
    ast.seriennr AS "Serial_Number__c",
    CASE
        WHEN ast.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(ast.garantie_bis::DATE, 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    k.kunden_nr AS "Account__c",
    MD5(p.proj_id) AS "Project__c",
    ast.asset_id AS "Legacy_Asset_ID__c",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "CreatedDate",
    TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS ast
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON ast.kd_ref = k.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
    ON ast.projekt_ref = p.proj_id