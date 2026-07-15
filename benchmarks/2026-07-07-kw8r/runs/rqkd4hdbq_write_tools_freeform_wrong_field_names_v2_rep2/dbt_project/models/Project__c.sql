{{ config(materialized='table') }}

SELECT
    MD5(proj_id) AS "Id",
    TRIM(name) AS "Name",
    CASE 
        WHEN LOWER(status) IN ('aktiv', 'active') THEN 'Active'
        WHEN LOWER(status) IN ('abgeschlossen', 'completed') THEN 'Completed'
        WHEN LOWER(status) IN ('in planung', 'in planning') THEN 'In Planning'
        WHEN LOWER(status) IN ('pausiert', 'on hold') THEN 'On Hold'
        WHEN LOWER(status) IN ('storniert', 'cancelled') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live
        WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN go_live ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(go_live, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(kd) AS "Account__c",
    MD5(opp) AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
