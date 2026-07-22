{{ config(materialized='table') }}

SELECT
    proj."proj_id" AS "Id",
    COALESCE(TRIM(proj."name"), 'Unknown') AS "Name",
    CASE
        WHEN LOWER(TRIM(proj."status")) = 'active' THEN 'Active'
        WHEN LOWER(TRIM(proj."status")) = 'completed' THEN 'Completed'
        WHEN LOWER(TRIM(proj."status")) = 'in planning' THEN 'In Planning'
        WHEN LOWER(TRIM(proj."status")) = 'on hold' THEN 'On Hold'
        WHEN LOWER(TRIM(proj."status")) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN TRIM(proj."go_live") ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(proj."go_live"), 'YYYY-MM-DD')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    LOWER(SUBSTR(MD5('acc_' || k."kunden_nr"), 1, 15)) AS "Account__c",
    proj."opp" AS "Opportunity__c",
    proj."proj_id" AS "Legacy_Project_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} proj
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(proj."kd") = TRIM(k."kunden_nr")