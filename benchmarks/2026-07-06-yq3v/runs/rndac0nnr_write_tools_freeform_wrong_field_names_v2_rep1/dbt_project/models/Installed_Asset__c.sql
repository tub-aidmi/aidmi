{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    COALESCE(a.bezeichnung, a.asset_id) AS "Name",
    a.seriennr AS "Serial_Number__c",
    CASE
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(a.garantie_bis AS DATE), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    k.kunden_nr AS "Account__c",
    p.proj_id AS "Project__c",
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS a
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON a.kd_ref = k.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
    ON a.projekt_ref = p.proj_id
