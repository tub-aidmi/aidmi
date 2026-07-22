{{ config(materialized='table') }}

SELECT
    -- Primary Key: Generate a unique ID for the installed asset
    UPPER(REPLACE(A.asset_kennung, ' ', '_')) AS "Id",

    -- Name of the asset
    A.asset_name AS "Name",

    -- Serial number of the asset
    A.serien_nummer AS "Serial_Number__c",

    -- Warranty End Date parsing various formats
    CASE
        WHEN A.garantieende ~ '^\d{4}-\d{2}-\d{2}$' THEN A.garantieende -- YYYY-MM-DD
        WHEN A.garantieende ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(A.garantieende, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
        WHEN A.garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(A.garantieende, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Foreign Key to Account.Id (derived from customer kennung)
    CASE WHEN A.kunden_kennung IS NOT NULL THEN 'ACC_' || UPPER(REPLACE(A.kunden_kennung, ' ', '_')) ELSE NULL END AS "Account__c",

    -- Foreign Key to Project__c.Id (derived from project kennung)
    CASE WHEN A.projekt_kennung IS NOT NULL THEN 'PRJ_' || UPPER(REPLACE(A.projekt_kennung, ' ', '_')) ELSE NULL END AS "Project__c",

    -- Legacy ID for verification
    A.asset_kennung AS "Legacy_Asset_ID__c",

    -- Standard Salesforce fields
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM
    {{ source('fixture_master_v2_src', 'master_assets') }} AS A
