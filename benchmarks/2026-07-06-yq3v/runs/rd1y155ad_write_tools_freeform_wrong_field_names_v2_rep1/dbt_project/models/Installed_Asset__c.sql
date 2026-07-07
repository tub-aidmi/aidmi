{{ config(materialized='table') }}

SELECT
    a.asset_id AS "Id",
    COALESCE(a.bezeichnung, 'N/A') AS "Name",
    a.seriennr AS "Serial_Number__c",
    COALESCE(
        TO_CHAR(
            TO_DATE(a.garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD'
        ),
        TO_CHAR(
            TO_DATE(a.garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD'
        ),
        NULL -- Prefer NULL for dates when no default is specified and it's nullable
    ) AS "Warranty_End_Date__c",
    a.kd_ref AS "Account__c", -- Account__c is kunden_nr from the kunden table
    a.projekt_ref AS "Project__c", -- Project__c is proj_id from the proj table
    a.asset_id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS a
WHERE
    a.asset_id IS NOT NULL
    AND COALESCE(a.bezeichnung, '') != ''
