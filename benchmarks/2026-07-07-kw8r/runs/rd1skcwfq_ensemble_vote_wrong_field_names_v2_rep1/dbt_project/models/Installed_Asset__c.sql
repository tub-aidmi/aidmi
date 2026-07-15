{{ config(materialized='table') }}

SELECT
    'ASSET-' || SUBSTRING(asset_id FROM 5) AS "Id",
    bezeichnung AS "Name",
    seriennr AS "Serial_Number__c",
    CASE
        WHEN garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN garantie_bis
        WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(garantie_bis, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    -- Map asset.kd_ref (CUST-xxxx) to Account.Id format (ACCT-xxxx)
    'ACCT-' || SUBSTRING(kd_ref FROM 6) AS "Account__c",
    -- Map asset.projekt_ref (PROJ-xxxxx) to Project__c.Id format (PROJ-xxxxx)
    'PROJ-' || SUBSTRING(projekt_ref FROM 6) AS "Project__c",
    asset_id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }}
