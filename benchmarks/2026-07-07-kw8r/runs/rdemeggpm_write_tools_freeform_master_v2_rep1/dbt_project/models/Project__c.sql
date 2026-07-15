{{ config(materialized='table') }}

SELECT
    -- Salesforce-style Project Id: prefix with 'P' for cross-table FK consistency
    'P' || TRIM("projekt_kennung") AS "Id",
    INITCAP(TRIM("projektname")) AS "Name",
    -- Map projektstatus to target enum, bilingual matching
    CASE
        WHEN UPPER(TRIM("projektstatus")) = 'AKTIV' THEN 'Active'
        WHEN UPPER(TRIM("projektstatus")) = 'ACTIVE' THEN 'Active'
        WHEN UPPER(TRIM("projektstatus")) = 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN UPPER(TRIM("projektstatus")) = 'COMPLETED' THEN 'Completed'
        WHEN UPPER(TRIM("projektstatus")) = 'FERTIG' THEN 'Completed'
        WHEN UPPER(TRIM("projektstatus")) = 'COMPLETE' THEN 'Completed'
        WHEN UPPER(TRIM("projektstatus")) = 'IN PLANUNG' THEN 'In Planning'
        WHEN UPPER(TRIM("projektstatus")) = 'IN PLANNING' THEN 'In Planning'
        WHEN UPPER(TRIM("projektstatus")) = 'PLANEN' THEN 'In Planning'
        WHEN UPPER(TRIM("projektstatus")) = 'PAUSIERT' THEN 'On Hold'
        WHEN UPPER(TRIM("projektstatus")) = 'ON HOLD' THEN 'On Hold'
        WHEN UPPER(TRIM("projektstatus")) = 'GEPAUSERT' THEN 'On Hold'
        WHEN UPPER(TRIM("projektstatus")) = 'GESTRICHT' THEN 'Cancelled'
        WHEN UPPER(TRIM("projektstatus")) = 'CANCELLED' THEN 'Cancelled'
        WHEN UPPER(TRIM("projektstatus")) = 'ABBRECHEN' THEN 'Cancelled'
        WHEN UPPER(TRIM("projektstatus")) = 'CANCELED' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    -- Parse go_live_datum (multiple possible formats) into ISO YYYY-MM-DD
    CASE
        WHEN TRIM("go_live_datum") IS NULL OR TRIM("go_live_datum") = '' THEN NULL
        WHEN TRIM("go_live_datum") ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM("go_live_datum"), 'DD.MM.YYYY')::TEXT
        WHEN TRIM("go_live_datum") ~ '^\d{4}-\d{2}-\d{2}$' THEN SUBSTRING(TRIM("go_live_datum") FROM 1 FOR 10)
        WHEN TRIM("go_live_datum") ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM("go_live_datum"), 'MM/DD/YYYY')::TEXT
        WHEN TRIM("go_live_datum") ~ '^\d{8}$' THEN
            CASE
                WHEN SUBSTRING(TRIM("go_live_datum") FROM 1 FOR 4)::INTEGER BETWEEN 1900 AND 2099
                    THEN TO_DATE(TRIM("go_live_datum"), 'YYYYMMDD')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    -- Account__c: Salesforce-style, match Account.Id = 'C' || kunden_kennung
    CASE
        WHEN TRIM("kunden_kennung") IS NOT NULL THEN 'C' || TRIM("kunden_kennung")
        ELSE NULL
    END AS "Account__c",
    -- Opportunity__c: Salesforce-style, match Opportunity.Id = 'O' || opp_kennung_ref
    CASE
        WHEN TRIM("opp_kennung_ref") IS NOT NULL THEN 'O' || TRIM("opp_kennung_ref")
        ELSE NULL
    END AS "Opportunity__c",
    -- Legacy key from source natural key
    TRIM("projekt_kennung") AS "Legacy_Project_ID__c",
    -- Fixed dates
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_projekte') }}
