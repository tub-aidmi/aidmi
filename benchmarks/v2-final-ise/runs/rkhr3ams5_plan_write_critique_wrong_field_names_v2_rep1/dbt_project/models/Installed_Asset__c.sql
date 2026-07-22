{{ config(materialized='table') }}

SELECT 
    -- Id: Transform asset_id to Salesforce Asset ID format (00u prefix + 15 digit number)
    '00u' || LPAD(REGEXP_REPLACE(TRIM(a.asset_id), '[^0-9]', '', 'g'), 15, '0') AS "Id",

    -- Name: bezeichnung with INITCAP and TRIM, fallback to 'Unnamed Asset' if NULL/empty
    COALESCE(NULLIF(INITCAP(TRIM(a.bezeichnung)), ''), 'Unnamed Asset') AS "Name",

    -- Serial_Number__c: raw serial number with trim
    TRIM(a.seriennr) AS "Serial_Number__c",

    -- Warranty_End_Date__c: parse multiple date formats to ISO YYYY-MM-DD, NULL if missing/unparseable
    CASE 
        WHEN a.garantie_bis ~ '^\d{4}-\d{2}-\d{2}$' THEN a.garantie_bis::DATE::TEXT
        WHEN a.garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(a.garantie_bis, 'DD.MM.YYYY')::TEXT
        WHEN a.garantie_bis ~ '^\d{8}$' THEN TO_DATE(a.garantie_bis, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: join with kunden and transform to Salesforce Account ID (001 prefix)
    CASE 
        WHEN TRIM(k.kunden_nr) IS NOT NULL THEN
            '001' || LPAD(REGEXP_REPLACE(TRIM(k.kunden_nr), '[^0-9]', '', 'g'), 15, '0')
        ELSE NULL
    END AS "Account__c",

    -- Project__c: join with proj and transform to Salesforce Project ID (00P prefix)
    CASE 
        WHEN TRIM(p.proj_id) IS NOT NULL THEN
            '00P' || LPAD(REGEXP_REPLACE(TRIM(p.proj_id), '[^0-9]', '', 'g'), 15, '0')
        ELSE NULL
    END AS "Project__c",

    -- Legacy_Asset_ID__c: original natural key for row-level traceability
    TRIM(a.asset_id) AS "Legacy_Asset_ID__c",

    -- Audit fields (defaults since not in source)
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(a.kd_ref) = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p 
    ON TRIM(a.projekt_ref) = TRIM(p.proj_id)