{{ config(materialized='table') }}

SELECT
    src."id" AS "Id",
    src."name" AS "Name",
    CASE INITCAP(TRIM(src."status"))
        WHEN 'Active' THEN 'Active'
        WHEN 'Completed' THEN 'Completed'
        WHEN 'In Planning' THEN 'In Planning'
        WHEN 'On Hold' THEN 'On Hold'
        WHEN 'Cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN src."go_live" IS NOT NULL AND TRIM(src."go_live") <> '' THEN
            CASE
                WHEN src."go_live" ~ '^\d{4}-\d{2}-\d{2}$' THEN src."go_live"
                ELSE NULL
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    -- Account__c: join to account via client_id (already ACC-xxx format)
    acc."id" AS "Account__c",
    -- Opportunity__c: use opportunity_ref directly (OPP-xxx format matches opportunity.id)
    src."opportunity_ref" AS "Opportunity__c",
    -- Legacy key
    src."id" AS "Legacy_Project_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'project') }} src
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON TRIM(src."client_id") = TRIM(acc."id")
