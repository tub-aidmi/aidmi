{{ config(materialized='table') }}

SELECT
    UPPER(TRIM(id)) AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unknown') AS "Name",
    UPPER(TRIM(serial)) AS "Serial_Number__c",
    CASE 
        WHEN warranty IS NOT NULL AND TRIM(warranty) != '' THEN
            CASE 
                WHEN TRIM(warranty) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(warranty), 'YYYY-MM-DD')::TEXT
                WHEN TRIM(warranty) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(warranty), 'DD.MM.YYYY')::TEXT
                ELSE NULL 
            END
        ELSE NULL 
    END AS "Warranty_End_Date__c",
    UPPER(TRIM(client)) AS "Account__c",
    UPPER(TRIM(project)) AS "Project__c",
    id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM "{{ source('fixture_missing_relations_v2_src', 'asset') }}"