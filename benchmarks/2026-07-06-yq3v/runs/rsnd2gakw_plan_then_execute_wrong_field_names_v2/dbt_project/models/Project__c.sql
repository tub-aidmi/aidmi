{{ config(materialized='table') }}

WITH projects_with_related_data AS (
    SELECT
        p.proj_id,
        p.name,
        p.status,
        p.go_live,
        k.kunden_nr AS account_kunden_nr,
        o.chance_id AS opportunity_chance_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
        ON p.kd = k.kunden_nr
    LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS o
        ON p.opp = o.chance_id
)
SELECT
    MD5(proj_id) AS "Id",
    COALESCE(name, 'Unnamed Project') AS "Name",
    CASE LOWER(TRIM(status))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live IS NULL OR TRIM(go_live) = '' THEN NULL
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(account_kunden_nr) AS "Account__c",
    MD5(opportunity_chance_id) AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM projects_with_related_data