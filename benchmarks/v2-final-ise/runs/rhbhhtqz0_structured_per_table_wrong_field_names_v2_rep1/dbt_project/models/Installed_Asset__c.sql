{{ config(materialized='table') }}

SELECT
    -- Generate Salesforce-style Asset Id (033 prefix is standard for Asset objects)
    '033' || LPAD(SUBSTRING(a.asset_id FROM '\d+')::INTEGER::TEXT, 6, '0') AS "Id",

    -- Asset name from bezeichnung
    a.bezeichnung AS "Name",

    -- Serial number
    a.seriennr AS "Serial_Number__c",

    -- Warranty end date (source is already in ISO YYYY-MM-DD format)
    a.garantie_bis AS "Warranty_End_Date__c",

    -- Account reference: inline generation must match Account model's Id format exactly
    '001' || LPAD(SUBSTRING(k.kunden_nr FROM '\d+')::INTEGER::TEXT, 6, '0') AS "Account__c",

    -- Project reference: inline generation matching Project__c model's Id format
    'a01' || LPAD(SUBSTRING(p.proj_id FROM '\d+')::INTEGER::TEXT, 6, '0') AS "Project__c",

    -- Legacy asset ID = original source natural key
    a.asset_id AS "Legacy_Asset_ID__c",

    -- System fields (not available in source data)
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} a

-- Join to customers for account relationship  
INNER JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON a.kd_ref = k.kunden_nr

-- Left join to projects (some assets may not have project association)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    ON a.projekt_ref = p.proj_id