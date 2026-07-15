{{ config(materialized='table') }}

SELECT
    -- Id from asset.id (also used for Legacy_Asset_ID__c)
    CAST(a.id AS TEXT) AS "Id",

    -- Name - normalize case, provide fallback for NULLs
    COALESCE(INITCAP(TRIM(a.name)), 'Unknown Asset') AS "Name",

    -- Serial Number
    TRIM(a.serial) AS "Serial_Number__c",

    -- Warranty End Date - parse multiple possible formats into ISO YYYY-MM-DD
    CASE
        WHEN a.warranty IS NULL THEN NULL
        WHEN a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.warranty, 'DD.MM.YYYY')::TEXT
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(a.warranty, 'YYYY-MM-DD')::TEXT
        WHEN a.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(a.warranty, 'MM/DD/YYYY')::TEXT
        WHEN a.warranty ~ '^\d{8}$' THEN TO_DATE(a.warranty, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c - join asset.client to account.id to get the Salesforce-style Account Id
    ac.id AS "Account__c",

    -- Project__c - join asset.project to project.id to get the Project Id
    p.id AS "Project__c",

    -- Legacy_Asset_ID__c from the source asset natural key (asset.id)
    a.id AS "Legacy_Asset_ID__c",

    -- CreatedDate - no source date provided, default to NULL
    NULL::TEXT AS "CreatedDate",

    -- LastModifiedDate - no source date provided, default to NULL
    NULL::TEXT AS "LastModifiedDate",

    -- IsDeleted - default to 0 (not deleted) since source has no delete flag
    0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} ac
    ON a.client = ac.id
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p
    ON a.project = p.id