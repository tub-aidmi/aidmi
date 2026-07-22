{{ config(materialized='table') }}

SELECT 
     '00I' || proj_id AS "Id",
    COALESCE(TRIM(name), '') AS "Name",
    CASE LOWER(TRIM(status))
        WHEN 'aktiv' THEN 'Active'
        WHEN 'active' THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'storniert' THEN 'Cancelled'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE 
        WHEN go_live IS NOT NULL AND TRIM(go_live) != '' THEN
            CASE 
                WHEN go_live ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(TRIM(go_live), 'FMDD.FM.MM.FMYYYY')::TEXT
                WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN go_live
                ELSE NULL
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
     '001' || TRIM(kd) AS "Account__c",
     '006' || TRIM(opp) AS "Opportunity__c",
    proj_id AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}