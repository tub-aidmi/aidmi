{{ config(materialized='table') }}

SELECT
    -- Id: Salesforce-style project ID derived from source projekt_kennung
    'a0Q' || REGEXP_REPLACE(p.projekt_kennung, '[^0-9]', '') AS "Id",

    -- Name: Project name from source (NOT NULL enforced via COALESCE)
    COALESCE(p.projektname, 'Unnamed Project') AS "Name",

    -- Project_Status__c: Mapped to enum (Active, Completed, In Planning, On Hold, Cancelled)
    CASE LOWER(TRIM(p.projektstatus))
        WHEN 'aktiv'          THEN 'Active'
        WHEN 'active'         THEN 'Active'
        WHEN 'abgeschlossen'  THEN 'Completed'
        WHEN 'completed'      THEN 'Completed'
        WHEN 'planung'        THEN 'In Planning'
        WHEN 'in planung'     THEN 'In Planning'
        WHEN 'in planning'    THEN 'In Planning'
        WHEN 'pausiert'       THEN 'On Hold'
        WHEN 'on hold'        THEN 'On Hold'
        WHEN 'cancelled'      THEN 'Cancelled'
        WHEN 'storniert'      THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",

    -- Go_Live_Date__c: Parse multiple date formats into ISO YYYY-MM-DD, with TRIM() on all branches
    CASE
        WHEN TRIM(p.go_live_datum) = '0000-00-00' THEN NULL
        WHEN TRIM(p.go_live_datum) ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(p.go_live_datum) ~ '^\d{8}$'
            THEN TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(p.go_live_datum) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_CHAR(TO_DATE(TRIM(p.go_live_datum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(p.go_live_datum) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TRIM(p.go_live_datum)
        ELSE NULL
    END AS "Go_Live_Date__c",

    -- Account__c: Salesforce-style Account Id mapped from kunden_kennung (fixed cross-table key)
    '001' || LOWER(REGEXP_REPLACE(p.kunden_kennung, '[^a-z0-9]', '', 'g')) AS "Account__c",

    -- Opportunity__c: Salesforce-style Opportunity Id mapped from opp_kennung_ref
    '006O' || REGEXP_REPLACE(p.opp_kennung_ref, '[^0-9]', '') AS "Opportunity__c",

    -- Legacy_Project_ID__c: Raw source natural key for traceability
    p.projekt_kennung AS "Legacy_Project_ID__c",

    -- CreatedDate / LastModifiedDate: NULL since source doesn't provide timestamps
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",

    -- IsDeleted: 0 = not deleted (source has no deletion flag)
    0 AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }} p