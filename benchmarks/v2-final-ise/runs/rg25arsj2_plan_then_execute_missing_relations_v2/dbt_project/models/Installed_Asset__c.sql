{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(TRIM(INITCAP(a.name)), 'Untitled Asset') AS "Name",
    TRIM(a.serial) AS "Serial_Number__c",
    CASE
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
        WHEN a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty ~ '^[0-9]{8}$' AND SUBSTR(a.warranty, 1, 4)::INTEGER BETWEEN 1900 AND 2100 THEN
            SUBSTR(a.warranty, 1, 4) || '-' || SUBSTR(a.warranty, 5, 2) || '-' || SUBSTR(a.warranty, 7, 2)
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acc.id AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM "{{ source('fixture_missing_relations_v2_src', 'asset') }}" a
LEFT JOIN "{{ source('fixture_missing_relations_v2_src', 'account') }}" acc
    ON (
        TRIM(UPPER(REGEXP_REPLACE(a.client, '[^A-Z0-9]', '', 'g'))) =
        TRIM(UPPER(REGEXP_REPLACE(acc.id, '[^A-Z0-9]', '', 'g')))
        OR
        TRIM(UPPER(a.client)) = TRIM(UPPER(acc.name))
    )
LEFT JOIN "{{ source('fixture_missing_relations_v2_src', 'project') }}" p
    ON TRIM(UPPER(REGEXP_REPLACE(a.project, '[^A-Z0-9]', '', 'g'))) =
       TRIM(UPPER(REGEXP_REPLACE(p.id, '[^A-Z0-9]', '', 'g')))