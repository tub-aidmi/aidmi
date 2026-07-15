{{ config(materialized='table') }}

SELECT 
    -- Salesforce-style Asset Id: validate 'A' prefix before transform; hash fallback (15 chars) for non-conforming keys
    CASE 
        WHEN a.asset_id IS NOT NULL AND a.asset_id LIKE 'A%' 
            THEN '00Q' || SUBSTRING(a.asset_id FROM 2)
        WHEN a.asset_id IS NOT NULL 
            THEN SUBSTRING(MD5(a.asset_id) FROM 1 FOR 15)
        ELSE NULL
    END AS "Id",

    -- Name: satisfy NOT NULL constraint with meaningful default
    COALESCE(TRIM(a.bezeichnung), 'Unknown Asset') AS "Name",

    a.seriennr AS "Serial_Number__c",

    /* Warranty_End_Date__c — parse DD.MM.YYYY safely with regex guard */
    CASE 
        WHEN TRIM(a.garantie_bis) ~ '^\d{2}\.\d{2}\.\d{4}$' 
            THEN REGEXP_REPLACE(TRIM(a.garantie_bis), '^(\d{2})\.(\d{2})\.(\d{4})$', '\3-\2-\1')
        ELSE NULL
    END AS "Warranty_End_Date__c",

    /* Account__c: validate 'K' prefix before transform; hash fallback for non-conforming keys */
    CASE 
        WHEN k.kunden_nr IS NOT NULL AND k.kunden_nr LIKE 'K%' 
            THEN '001' || SUBSTRING(k.kunden_nr FROM 2)
        WHEN k.kunden_nr IS NOT NULL 
            THEN SUBSTRING(MD5(k.kunden_nr) FROM 1 FOR 15)
        ELSE NULL
    END AS "Account__c",

    /* Project__c: validate 'P' prefix before transform; hash fallback for non-conforming keys */
    CASE 
        WHEN p.proj_id IS NOT NULL AND p.proj_id LIKE 'P%' 
            THEN '00H' || SUBSTRING(p.proj_id FROM 2)
        WHEN p.proj_id IS NOT NULL 
            THEN SUBSTRING(MD5(p.proj_id) FROM 1 FOR 15)
        ELSE NULL
    END AS "Project__c",

    /* Legacy_Asset_ID__c: raw source asset key for row-level traceability */
    TRIM(a.asset_id) AS "Legacy_Asset_ID__c",

    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a

LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k 
    ON TRIM(a.kd_ref) = TRIM(k.kunden_nr)

LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p 
    ON TRIM(a.projekt_ref) = TRIM(p.proj_id)