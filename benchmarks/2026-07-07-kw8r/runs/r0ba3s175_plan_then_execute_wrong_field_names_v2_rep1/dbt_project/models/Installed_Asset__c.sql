{{ config(materialized='table') }}

SELECT
    -- Id: Primary key from asset_id (normalized)
    UPPER(TRIM(asset_id)) AS "Id",

    -- Name: Asset description; default for NOT NULL constraint
    COALESCE(UPPER(TRIM(bezeichnung)), 'Unknown Asset') AS "Name",

    -- Serial_Number__c: From seriennr
    UPPER(TRIM(seriennr)) AS "Serial_Number__c",

    -- Warranty_End_Date__c: Parse garantie_bis for DD.MM.YYYY, YYYY-MM-DD, or YYYYMMDD
    CASE
        WHEN TRIM(garantie_bis) IS NULL OR TRIM(garantie_bis) = '' THEN NULL
        WHEN TRIM(garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(garantie_bis), 'YYYY-MM-DD')::TEXT
        WHEN TRIM(garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(garantie_bis), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(garantie_bis) ~ '^\d{8}$' THEN TO_DATE(TRIM(garantie_bis), 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: Normalized kunden_nr from join (represents Salesforce Account.Id)
    UPPER(TRIM(a.kunden_nr)) AS "Account__c",

    -- Project__c: Normalized proj_id from join (represents Salesforce Project.Id)
    UPPER(TRIM(p.proj_id)) AS "Project__c",

    -- Legacy_Asset_ID__c: Original asset_id for row-level traceability
    UPPER(TRIM(asset_id)) AS "Legacy_Asset_ID__c",

    -- CreatedDate: Source lacks timestamps; use current date
    CURRENT_DATE::TEXT AS "CreatedDate",

    -- LastModifiedDate: Source lacks timestamps; use current date
    CURRENT_DATE::TEXT AS "LastModifiedDate",

    -- IsDeleted: Hardcoded to 0 (no deletion flags in source)
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} assets

-- Join with kunden to resolve Account__c via normalized kd_ref
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} a
    ON UPPER(TRIM(assets.kd_ref)) = UPPER(TRIM(a.kunden_nr))

-- Join with proj to resolve Project__c via normalized projekt_ref
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    ON UPPER(TRIM(assets.projekt_ref)) = UPPER(TRIM(p.proj_id))