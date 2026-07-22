{{ config(materialized='table') }}

SELECT
    CAST(a.id AS TEXT) AS "Id",
    CAST(a.name AS TEXT) AS "Name",
    CAST(a.serial AS TEXT) AS "Serial_Number__c",
    COALESCE(
        CASE WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' 
             THEN TO_CHAR(TO_DATE(a.warranty, 'YYYY-MM-DD'), 'YYYY-MM-DD') ELSE NULL END,
        CASE WHEN a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' 
             THEN TO_CHAR(TO_DATE(a.warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD') ELSE NULL END,
        CASE WHEN a.warranty ~ '^\d{8}$' 
             THEN TO_CHAR(TO_DATE(a.warranty, 'YYYYMMDD'), 'YYYY-MM-DD') ELSE NULL END,
        CASE WHEN a.warranty ~ '^\d{2}/\d{2}/\d{4}$' 
             THEN TO_CHAR(TO_DATE(a.warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD') ELSE NULL END
    ) AS "Warranty_End_Date__c",
    COALESCE(
        (SELECT id FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
         WHERE LOWER(TRIM(id)) = LOWER(TRIM(a.client)) LIMIT 1),
        (SELECT id FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
         WHERE LOWER(TRIM(name)) = LOWER(TRIM(a.client)) LIMIT 1)
    ) AS "Account__c",
    CAST(p.id AS TEXT) AS "Project__c",
    CAST(a.id AS TEXT) AS "Legacy_Asset_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p
    ON a.project = p.id