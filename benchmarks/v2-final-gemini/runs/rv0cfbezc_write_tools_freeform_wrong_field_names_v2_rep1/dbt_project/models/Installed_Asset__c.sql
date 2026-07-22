{{ config(materialized='table') }}

SELECT
    asset_id AS "Id",
    COALESCE(bezeichnung, 'Unnamed Asset') AS "Name",
    seriennr AS "Serial_Number__c",
    -- Date parsing: assuming YYYY-MM-DD, DD.MM.YYYY, YYYYMMDD, MM/DD/YYYY formats
    CASE
        WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN garantie_bis
        WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantie_bis ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    kd_ref AS "Account__c", -- Maps to Account.Id (kunden_nr)
    projekt_ref AS "Project__c", -- Maps to Project__c.Id (proj_id)
    asset_id AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
