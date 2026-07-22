{{ config(materialized='table') }}

SELECT
    a.id AS "Id",
    COALESCE(NULLIF(a.name, ''), 'Unknown') AS "Name",
    TRIM(a.serial) AS "Serial_Number__c",
    CASE
        WHEN a.warranty IS NOT NULL AND TRIM(a.warranty) != '' THEN
            TO_CHAR(
                TO_DATE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(a.warranty, '([0-9]{2})[/.]([0-9]{2})[/.]([0-9]{4})', '\3-\2-\1'),
                        '([0-9]{4})([0-9]{2})([0-9]{2})',
                        '\1-\2-\3'
                    ),
                    'YYYY-MM-DD'
                ),
                'YYYY-MM-DD'
            )
        ELSE NULL
    END AS "Warranty_End_Date__c",
    acc.id AS "Account__c",
    p.id AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'asset') }} a
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON a.client = acc.id OR a.client = acc.name
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'project') }} p
    ON a.project = p.id OR a.project = p.name
