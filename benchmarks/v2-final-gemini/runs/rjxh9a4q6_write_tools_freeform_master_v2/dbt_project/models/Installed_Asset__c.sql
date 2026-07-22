{{ config(materialized='table') }}

SELECT
    asset_kennung AS "Id",
    COALESCE(TRIM(asset_name), asset_kennung) AS "Name", -- Name is NOT NULL
    TRIM(serien_nummer) AS "Serial_Number__c",
    COALESCE(
        TO_CHAR(
            TO_DATE(TRIM(garantieende), 'YYYY-MM-DD'), 'YYYY-MM-DD'
        ),
        TO_CHAR(
            TO_DATE(TRIM(garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD'
        ),
        TO_CHAR(
            TO_DATE(TRIM(garantieende), 'YYYYMMDD'), 'YYYY-MM-DD'
        ),
        NULL
    ) AS "Warranty_End_Date__c",
    kunden_kennung AS "Account__c", -- Maps to Account.Id
    projekt_kennung AS "Project__c", -- Maps to Project__c.Id
    asset_kennung AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_assets') }}
