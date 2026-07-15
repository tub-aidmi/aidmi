{{ config(materialized='table') }}

SELECT
    TRIM(a.id) AS "Id",
    COALESCE(TRIM(a.name), 'Unknown') AS "Name",
    a.serial AS "Serial_Number__c",
    CASE
        WHEN a.warranty IS NOT NULL AND TRIM(a.warranty) != '' THEN
            CASE
                WHEN a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(a.warranty), 'DD.MM.YYYY')::TEXT
                WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(a.warranty), 'YYYY-MM-DD')::TEXT
                WHEN a.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(a.warranty), 'MM/DD/YYYY')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "Warranty_End_Date__c",
    TRIM(ac.id) AS "Account__c",
    TRIM(p.id) AS "Project__c",
    TRIM(a.id) AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} ac
    ON TRIM(a.client) = TRIM(ac.id)
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p
    ON TRIM(a.project) = TRIM(p.id)