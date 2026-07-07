-- models/Installed_Asset__c.sql

{{ config(materialized='table') }}

SELECT
    asset_id AS "Id",
    bezeichnung AS "Name",
    seriennr AS "Serial_Number__c",
    COALESCE(
        CASE
            WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            WHEN garantie_bis ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'YYYYMMDD'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '2000-01-01' -- Default date for Warranty_End_Date__c
    ) AS "Warranty_End_Date__c",
    kd_ref AS "Account__c", -- Raw kunden_nr
    projekt_ref AS "Project__c", -- Raw proj_id
    asset_id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
