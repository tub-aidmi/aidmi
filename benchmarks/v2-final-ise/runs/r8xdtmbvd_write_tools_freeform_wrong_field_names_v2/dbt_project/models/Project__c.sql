{{ config(materialized='table') }}

SELECT
    proj_id AS "Id",
    name AS "Name",
    CASE
        WHEN UPPER(status) IN ('ACTIVE', 'COMPLETED', 'IN PLANNING', 'ON HOLD', 'CANCELLED') THEN INITCAP(LOWER(status))
        WHEN UPPER(status) IN ('AKTIV') THEN 'Active'
        WHEN UPPER(status) IN ('ABGESCHLOSSEN') THEN 'Completed'
        WHEN UPPER(status) IN ('IN PLANUNG') THEN 'In Planning'
        WHEN UPPER(status) IN ('PAUSIERT', 'ANGEHALTEN') THEN 'On Hold'
        WHEN UPPER(status) IN ('STORNIERT') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    k.kunden_nr AS "Account__c",
    c.chance_id AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k ON p.kd = k.kunden_nr
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c ON p.opp = c.chance_id
