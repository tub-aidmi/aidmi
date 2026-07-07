{{ config(materialized='table') }}

SELECT
    -- Primary Key: Generate a stable ID using MD5 hash of the source project identifier.
    MD5(p.projekt_kennung) AS "Id",

    -- Project Name: Use projectname if available, otherwise construct a name from the project identifier.
    COALESCE(p.projektname, 'Unnamed Project ' || p.projekt_kennung) AS "Name",

    -- Project Status: Map source status to target enum values. Handle German terms and case-insensitivity.
    CASE
        WHEN LOWER(p.projektstatus) = 'active' THEN 'Active'
        WHEN LOWER(p.projektstatus) = 'completed' OR LOWER(p.projektstatus) = 'abgeschlossen' THEN 'Completed'
        WHEN LOWER(p.projektstatus) = 'in planning' OR LOWER(p.projektstatus) = 'planung' THEN 'In Planning'
        WHEN LOWER(p.projektstatus) = 'on hold' THEN 'On Hold'
        WHEN LOWER(p.projektstatus) = 'cancelled' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",

    -- Go-Live Date: Parse various date formats from the source and output in ISO YYYY-MM-DD format.
    CASE
        WHEN p.go_live_datum IS NULL OR TRIM(p.go_live_datum) = '' OR p.go_live_datum = '0000-00-00' THEN NULL
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{1,2}\/\d{1,2}\/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- Handles M/D/YYYY and MM/DD/YYYY
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",

    -- Account Foreign Key: Generate Account ID using MD5 hash of the customer identifier, consistent with Account model.
    MD5(p.kunden_kennung) AS "Account__c",

    -- Opportunity Foreign Key: Generate Opportunity ID using MD5 hash of the opportunity identifier, consistent with Opportunity model.
    MD5(p.opp_kennung_ref) AS "Opportunity__c",

    -- Legacy Project ID: Direct mapping of the source natural key for verification.
    p.projekt_kennung AS "Legacy_Project_ID__c",

    -- CreatedDate: Placeholder with current timestamp.
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",

    -- LastModifiedDate: Placeholder with current timestamp.
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",

    -- IsDeleted: Default to 0 as no source indicates deletion status.
    0 AS "IsDeleted"

FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
