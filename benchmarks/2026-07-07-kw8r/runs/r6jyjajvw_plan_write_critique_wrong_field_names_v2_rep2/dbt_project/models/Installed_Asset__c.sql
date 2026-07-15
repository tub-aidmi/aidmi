{{ config(materialized='table') }}

SELECT
    -- Id <- asset_id (Direct copy, NOT NULL)
    a.asset_id AS "Id",

    -- Name <- bezeichnung (INITCAP/TRIM; fallback 'Unnamed Asset' for NULL/empty)
    COALESCE(INITCAP(TRIM(a.bezeichnung)), 'Unnamed Asset') AS "Name",

    -- Serial_Number__c <- seriennr (UPPER + TRIM for standardization/uniqueness)
    UPPER(TRIM(a.seriennr)) AS "Serial_Number__c",

    -- Warranty_End_Date__c <- garantie_bis
    -- All observed dates are YYYY-MM-DD; handle additional DD.MM.YYYY / YYYYMMDD per guidelines
    CASE
        WHEN TRIM(a.garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE(TRIM(a.garantie_bis), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(a.garantie_bis) ~ '^\d{8}$'
            THEN TO_DATE(TRIM(a.garantie_bis), 'YYYYMMDD')::TEXT
        WHEN TRIM(a.garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_DATE(TRIM(a.garantie_bis), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c <- kunden.kunden_nr (LEFT JOIN via kd_ref = kunden_nr; same transform as Account.Id)
    k.kunden_nr AS "Account__c",

    -- Project__c <- proj.proj_id (LEFT JOIN via projekt_ref = proj_id; transformed Project__c.Id)
    p.proj_id AS "Project__c",

    -- Legacy_Asset_ID__c <- asset_id (exact source natural key)
    a.asset_id AS "Legacy_Asset_ID__c",

    -- Audit columns — no source data available; static placeholders
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(k.kunden_nr) = TRIM(a.kd_ref)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    ON TRIM(p.proj_id) = TRIM(a.projekt_ref)