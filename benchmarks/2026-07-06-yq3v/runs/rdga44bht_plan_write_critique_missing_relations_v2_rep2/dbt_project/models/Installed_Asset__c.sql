{{ config(materialized='table') }}

WITH account_name_lookup AS (
    SELECT
        name,
        MIN(id) AS id
    FROM
        {{ source('fixture_missing_relations_v2_src', 'account') }}
    GROUP BY
        name
), project_name_lookup AS (
    SELECT
        name,
        MIN(id) AS id
    FROM
        {{ source('fixture_missing_relations_v2_src', 'project') }}
    GROUP BY
        name
)
SELECT
    a.id AS "Id",
    COALESCE(a.name, 'Untitled Asset') AS "Name",
    a.serial AS "Serial_Number__c",
    CASE
        WHEN a.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN a.warranty
        WHEN a.warranty ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(a.warranty, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN a.warranty ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(a.warranty, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(
        ac_id.id,
        ac_name_lkp.id
    ) AS "Account__c",
    COALESCE(
        p_id.id,
        p_name_lkp.id
    ) AS "Project__c",
    a.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS a
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS ac_id
    ON a.client = ac_id.id
LEFT JOIN
    account_name_lookup AS ac_name_lkp
    ON ac_id.id IS NULL AND a.client = ac_name_lkp.name
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p_id
    ON a.project = p_id.id
LEFT JOIN
    project_name_lookup AS p_name_lkp
    ON p_id.id IS NULL AND a.project = p_name_lkp.name