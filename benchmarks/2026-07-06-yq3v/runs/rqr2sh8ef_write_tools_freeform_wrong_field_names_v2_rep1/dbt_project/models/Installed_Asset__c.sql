{{ config(materialized='table') }}

SELECT
    asset_id AS "Id",
    COALESCE(bezeichnung, 'Unknown Asset') AS "Name", -- Name is NOT NULL
    seriennr AS "Serial_Number__c",
    CASE
        WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    kd_ref AS "Account__c", -- Corresponds to Account.Id (kunden_nr)
    projekt_ref AS "Project__c", -- Corresponds to Project__c.Id (proj_id)
    asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
