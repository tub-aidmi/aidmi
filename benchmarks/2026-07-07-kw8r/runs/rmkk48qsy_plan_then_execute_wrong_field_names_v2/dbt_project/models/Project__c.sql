{{ config(materialized='table') }}

SELECT
    COALESCE('a00' || UPPER(TRIM(proj_id)), 'a00UNKNOWN') AS "Id",
    COALESCE(TRIM(INITCAP(name)), 'Unnamed Project') AS "Name",
    CASE
        WHEN UPPER(TRIM(status)) LIKE '%AKTIV%' OR UPPER(TRIM(status)) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM(status)) LIKE '%ABSCHL%' OR UPPER(TRIM(status)) LIKE '%FERTIG%' OR UPPER(TRIM(status)) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM(status)) LIKE '%PLANUNG%' OR UPPER(TRIM(status)) LIKE '%GEPLANT%' OR UPPER(TRIM(status)) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM(status)) LIKE '%PAUSIER%' OR UPPER(TRIM(status)) LIKE '%ANGEHALT%' OR UPPER(TRIM(status)) LIKE '%HOLD%' THEN 'On Hold'
        WHEN UPPER(TRIM(status)) LIKE '%STORNIER%' OR UPPER(TRIM(status)) LIKE '%ABGEBR%' OR UPPER(TRIM(status)) LIKE '%CANCEL%' THEN 'Cancelled'
        ELSE 'In Planning'
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(COALESCE(go_live, '')) = '' THEN NULL
        WHEN TRIM(go_live) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN CAST(TO_DATE(TRIM(go_live), 'DD.MM.YYYY') AS TEXT)
        WHEN TRIM(go_live) ~ '^\d{8}$' THEN CAST(TO_DATE(TRIM(go_live), 'YYYYMMDD') AS TEXT)
        WHEN TRIM(go_live) ~ '^\d{4}-\d{2}-\d{2}$' THEN TRIM(go_live)
        ELSE NULL
    END AS "Go_Live_Date__c",
    CASE WHEN TRIM(COALESCE(kd, '')) = '' THEN NULL ELSE '001' || UPPER(TRIM(kd)) END AS "Account__c",
    CASE WHEN TRIM(COALESCE(opp, '')) = '' THEN NULL ELSE '006' || UPPER(TRIM(opp)) END AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}