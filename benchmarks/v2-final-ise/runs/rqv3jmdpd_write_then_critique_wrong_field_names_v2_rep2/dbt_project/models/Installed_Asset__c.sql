{{ config(materialized='table') }}

SELECT
    -- Id: use source asset_id directly as primary key
    a.asset_id AS "Id",

    -- Name: from asset bezeichnung, default for NULLs since NOT NULL constraint
    COALESCE(a.bezeichnung, 'Unnamed Asset') AS "Name",

    -- Serial_Number__c: from source seriennr
    a.seriennr AS "Serial_Number__c",

    -- Warranty_End_Date__c: parse garantie_bis (handle DD.MM.YYYY and YYYYMMDD formats)
    CASE
        WHEN a.garantie_bis IS NULL THEN NULL
         -- DD.MM.YYYY or D.M.YYYY — use LPAD to zero-pad day and month before TO_DATE
        WHEN a.garantie_bis ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
            TO_DATE(
                LPAD(SPLIT_PART(a.garantie_bis, '.', 1), 2, '0') || '.' ||
                LPAD(SPLIT_PART(a.garantie_bis, '.', 2), 2, '0') || '.' ||
                SPLIT_PART(a.garantie_bis, '.', 3),
                 'DD.MM.YYYY'
            )::TEXT
         -- YYYYMMDD — string-based formatting, no TO_DATE needed
        WHEN a.garantie_bis ~ '^\d{8}$' THEN
            SUBSTR(a.garantie_bis, 1, 4) || '-' ||
            SUBSTR(a.garantie_bis, 5, 2) || '-' ||
            SUBSTR(a.garantie_bis, 7, 2)
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: transform kd_ref to Salesforce-style Account Id
    -- Simple prefix prepend (consistent with Account model transformation)
    CASE
        WHEN a.kd_ref IS NOT NULL THEN '001' || TRIM(a.kd_ref)
        ELSE NULL
    END AS "Account__c",

    -- Project__c: transform projekt_ref to Salesforce-style custom object Id
    -- Simple prefix prepend (consistent with Project model transformation)
    CASE
        WHEN a.projekt_ref IS NOT NULL THEN '00I' || TRIM(a.projekt_ref)
        ELSE NULL
    END AS "Project__c",

    -- Legacy_Asset_ID__c: preserve the original source key
    a.asset_id AS "Legacy_Asset_ID__c",

    -- CreatedDate/LastModifiedDate: not available in source data, set to NULL
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",

    -- IsDeleted: default to 0 (not deleted)
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a