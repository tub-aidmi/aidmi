{{ config(materialized='table') }}

SELECT
    UPPER(TRIM(a.id)) AS "Id",
    COALESCE(INITCAP(TRIM(a.name)), 'Unknown') AS "Name",
    UPPER(TRIM(a.serial)) AS "Serial_Number__c",
    CASE 
        WHEN a.warranty IS NOT NULL AND TRIM(a.warranty) != '' THEN
            CASE 
                WHEN TRIM(a.warranty) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(a.warranty), 'YYYY-MM-DD')::TEXT
                WHEN TRIM(a.warranty) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.warranty), 'DD.MM.YYYY')::TEXT
                ELSE NULL 
            END
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    COALESCE(ac_by_id.id, ac_by_name.id) AS "Account__c",
    UPPER(TRIM(p.id)) AS "Project__c",
    UPPER(TRIM(a.id)) AS "Legacy_Asset_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} ac_by_id 
    ON UPPER(TRIM(ac_by_id.id)) = UPPER(TRIM(a.client))
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} ac_by_name 
    ON LOWER(TRIM(ac_by_name.name)) = LOWER(TRIM(a.client)) 
    AND ac_by_id.id IS NULL
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p 
    ON UPPER(TRIM(p.id)) = UPPER(TRIM(a.project))