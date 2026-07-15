{{ config(materialized='table') }}

SELECT
    asset_id AS "Id",
    bezeichnung AS "Name",
    seriennr AS "Serial_Number__c",
    CASE 
        WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN garantie_bis
        WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantie_bis ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    kunden."Id" AS "Account__c",
    proj."Id" AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} AS assets
LEFT JOIN (
    SELECT 
        kunden_nr AS "Legacy_Customer_ID__c",
        kunden_nr AS "Id"
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
) AS kunden ON assets.kd_ref = kunden."Legacy_Customer_ID__c"
LEFT JOIN (
    SELECT 
        proj_id AS "Legacy_Project_ID__c",
        proj_id AS "Id"
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
) AS proj ON assets.projekt_ref = proj."Legacy_Project_ID__c"