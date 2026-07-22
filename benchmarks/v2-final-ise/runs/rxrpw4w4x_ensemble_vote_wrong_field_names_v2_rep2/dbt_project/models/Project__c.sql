{{ config(materialized='table') }}

SELECT
    -- Id: Use the project natural key as-is for the Salesforce-style Id
    proj_id AS "Id",

    -- Name: Direct mapping from source name
    name AS "Name",

    -- Project_Status__c: Map source status values to target enum domain
    CASE LOWER(TRIM(status))
        WHEN 'active' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",

    -- Go_Live_Date__c: Parse the source date string (assumes DD.MM.YYYY or YYYY-MM-DD format)
    CASE
        WHEN go_live IS NOT NULL AND go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
            TO_CHAR(TO_DATE(go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live IS NOT NULL AND go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN
            go_live
        ELSE NULL
    END AS "Go_Live_Date__c",

    -- Account__c: Map source customer number (kd) to the Salesforce Account Id.
    -- We concatenate a standard Salesforce-style prefix so it references the corresponding Account record.
    '001' || SUBSTRING(
        CASE
            WHEN kd ~ '^\d+$' THEN LPAD(kd, 18 - 3 - LENGTH('001'), '0')
            ELSE REPEAT('0', 15)
        END,
        1, 15
    ) AS "Account__c",

    -- Opportunity__c: Map source opportunity reference (opp) to the Salesforce Opportunity Id.
    -- We concatenate a standard Salesforce-style prefix so it references the corresponding Opportunity record.
    '006' || SUBSTRING(
        CASE
            WHEN opp ~ '^\d+$' THEN LPAD(opp, 18 - 3 - LENGTH('006'), '0')
            ELSE REPEAT('0', 15)
        END,
        1, 15
    ) AS "Opportunity__c",

    -- Legacy_Project_ID__c: The natural key used to verify row-level correctness
    proj_id AS "Legacy_Project_ID__c",

    -- CreatedDate / LastModifiedDate: Not available in source; set to NULL per policy (prefer NULL over sentinel dates)
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",

    -- IsDeleted: Default to 0 (not deleted)
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}