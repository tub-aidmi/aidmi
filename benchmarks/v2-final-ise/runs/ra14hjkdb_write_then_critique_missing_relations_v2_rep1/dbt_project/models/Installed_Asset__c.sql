{{ config(materialized='table') }}

SELECT
    -- Id: preserve source AST- prefix to match canonical id format used across all models
    asset.id AS "Id",

    -- Name: fallback to 'Unknown Asset' when missing/empty
    COALESCE(TRIM(asset.name), 'Unknown Asset') AS "Name",

    -- Serial_Number__c: clean whitespace
    TRIM(asset.serial) AS "Serial_Number__c",

    -- Warranty_End_Date__c: normalize multiple date formats to YYYY-MM-DD or NULL
    CASE
        WHEN asset.warranty IS NOT NULL AND TRIM(asset.warranty) <> '' THEN
            CASE
                WHEN TRIM(asset.warranty) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(asset.warranty)
                WHEN TRIM(asset.warranty) ~ '^\d{2}\.\d{2}\.\d{4}$'
                    THEN TO_CHAR(TO_DATE(TRIM(asset.warranty), 'DD.MM.YYYY'), 'YYYY-MM-DD')
                WHEN TRIM(asset.warranty) ~ '^\d{8}$'
                    THEN SUBSTR(TRIM(asset.warranty), 1, 4) || '-'
                         || SUBSTR(TRIM(asset.warranty), 5, 2) || '-'
                         || SUBSTR(TRIM(asset.warranty), 7, 2)
                ELSE NULL
            END
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: resolve account and preserve the canonical ACC- prefixed Id for referential consistency.
    -- Matches via direct ACC- prefix first; falls back to matching by company name.
    CASE
        WHEN acc.id IS NOT NULL THEN acc.id
        ELSE NULL
    END AS "Account__c",

    -- Project__c: resolve project and preserve the canonical PROJ- prefixed Id for referential consistency.
    CASE
        WHEN proj.id IS NOT NULL THEN proj.id
        ELSE NULL
    END AS "Project__c",

    -- Legacy_Asset_ID__c: raw source natural key for row-level verification
    asset.id AS "Legacy_Asset_ID__c",

    -- Dates not present in source; use NULL
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",

    -- Default IsDeleted to false (0)
    0::integer AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} asset

-- Join account: prefer matching by canonical ACC- ID; fallback to company name match
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON (TRIM(asset.client) LIKE 'ACC-%' AND TRIM(acc.id) = TRIM(asset.client))
       OR (TRIM(asset.client) NOT LIKE 'ACC-%' AND TRIM(acc.name) = TRIM(asset.client))

-- Join project: match by canonical PROJ- ID
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} proj
    ON TRIM(proj.id) = TRIM(asset.project)