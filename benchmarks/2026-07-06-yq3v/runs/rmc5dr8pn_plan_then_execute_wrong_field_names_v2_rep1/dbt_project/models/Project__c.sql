-- This dbt model transforms raw project data into the Project__c Salesforce-like schema.
{{ config(materialized='table') }}

SELECT
    p.proj_id AS "Id",
    COALESCE(p.name, 'Unnamed Project') AS "Name",
    CASE UPPER(TRIM(p.status))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        ELSE 'In Planning' -- Default as per plan
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN p.go_live -- Already YYYY-MM-DD
        WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    k.kunden_nr AS "Account__c",
    c.chance_id AS "Opportunity__c",
    p.proj_id AS "Legacy_Project_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'proj') }} AS p
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON p.kd = k.kunden_nr
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
    ON p.opp = c.chance_id