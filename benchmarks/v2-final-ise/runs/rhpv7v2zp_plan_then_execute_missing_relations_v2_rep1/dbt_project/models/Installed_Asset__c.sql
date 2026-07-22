{{ config(materialized='table') }}

SELECT
    UPPER(TRIM("id")) AS "Id",
    COALESCE(INITCAP(NULLIF(TRIM("name"), '')), 'Unknown') AS "Name",
    UPPER(TRIM("serial")) AS "Serial_Number__c",
    CASE 
        WHEN "warranty" IS NOT NULL THEN
            COALESCE(
                CASE WHEN "warranty" ~ '^\d{8}$' THEN TO_DATE("warranty", 'YYYYMMDD')::TEXT END,
                CASE WHEN "warranty" ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE("warranty", 'DD.MM.YYYY')::TEXT END,
                CASE WHEN "warranty" ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE("warranty", 'MM/DD/YYYY')::TEXT END,
                NULL
            )
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    UPPER(TRIM("client")) AS "Account__c",
    UPPER(TRIM("project")) AS "Project__c",
    TRIM("id") AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }}