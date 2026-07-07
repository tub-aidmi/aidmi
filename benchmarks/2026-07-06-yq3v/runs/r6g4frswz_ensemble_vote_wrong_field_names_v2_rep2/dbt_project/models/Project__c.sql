{{ config(materialized='table') }}

SELECT
    MD5(p.proj_id) AS "Id",
    COALESCE(TRIM(p.name), 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(TRIM(p.status)) IN ('aktiv', 'in bearbeitung') THEN 'Active'
        WHEN LOWER(TRIM(p.status)) IN ('abgeschlossen', 'fertig') THEN 'Completed'
        WHEN LOWER(TRIM(p.status)) = 'in planung' THEN 'In Planning'
        WHEN LOWER(TRIM(p.status)) IN ('pausiert', 'wartend') THEN 'On Hold'
        WHEN LOWER(TRIM(p.status)) IN ('abgebrochen', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(TO_DATE(p.go_live, 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(p.go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
    ) AS "Go_Live_Date__c",
    MD5(k.kunden_nr) AS "Account__c",
    MD5(c.chance_id) AS "Opportunity__c",
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