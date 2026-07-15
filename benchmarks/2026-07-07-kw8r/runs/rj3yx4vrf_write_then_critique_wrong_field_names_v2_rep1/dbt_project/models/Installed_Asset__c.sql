{{ config(materialized='table') }}

SELECT 
    -- Salesforce-style Id for Asset: use 00R prefix with zero-padded source key
    '00R' || LPAD(TRIM(a.asset_id), 12, '0') AS "Id",

    -- Asset name / description (NOT NULL constraint)
    COALESCE(TRIM(a.bezeichnung), 'Unnamed Asset') AS "Name",

    -- Serial number
    TRIM(a.seriennr) AS "Serial_Number__c",

    -- Warranty end date: parse DD.MM.YYYY or YYYY-MM-DD formats, return ISO text
    CASE 
        WHEN TRIM(a.garantie_bis) = '' THEN NULL
        WHEN TRIM(a.garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.garantie_bis), 'DD.MM.YYYY')::TEXT
        WHEN TRIM(a.garantie_bis) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(a.garantie_bis), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account reference: match Account.Id transform = '001' || LPAD(kunden_nr, 12, '0')
    CASE 
        WHEN k.kunden_nr IS NOT NULL THEN '001' || LPAD(TRIM(k.kunden_nr), 12, '0')
        ELSE NULL
    END AS "Account__c",

    -- Project reference: match Project__c.Id transform = '500' || LPAD(proj_id, 12, '0')
    CASE 
        WHEN p.proj_id IS NOT NULL THEN '500' || LPAD(TRIM(p.proj_id), 12, '0')
        ELSE NULL
    END AS "Project__c",

    -- Legacy natural key for row-level verification
    TRIM(a.asset_id) AS "Legacy_Asset_ID__c",

    -- No source timestamp columns — leave as NULL
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",

    -- Deletion flag: 0 = active record
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(a.kd_ref) = TRIM(k.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    ON TRIM(a.projekt_ref) = TRIM(p.proj_id)