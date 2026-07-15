{{ config(materialized='table') }}

/*
 * Project__c: Transforms source project data from fixture_wrong_field_names_v2_src.proj
 * into Salesforce-style Project__c records with proper key lookups for Account and Opportunity.
 */

WITH proj_raw AS (
    SELECT
        proj_id,
        name,
        status,
        go_live,
        kd,
        opp
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
),

-- Generate deterministic Salesforce-style Account IDs from kunden natural key
accounts AS (
    SELECT
        TRIM(kunden_nr) AS kunden_key,
        CONCAT('001', SUBSTRING(MD5(TRIM(kunden_nr)), 1, 13)) AS account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
    WHERE kunden_nr IS NOT NULL
      AND TRIM(kunden_nr) != ''
),

-- Generate deterministic Salesforce-style Opportunity IDs from chancen natural key
opportunities AS (
    SELECT
        TRIM(chance_id) AS chance_key,
        CONCAT('006', SUBSTRING(MD5(TRIM(chance_id)), 1, 13)) AS opportunity_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
    WHERE chance_id IS NOT NULL
      AND TRIM(chance_id) != ''
),

-- Project records with joined Account and Opportunity lookups
projects AS (
    SELECT
        p.proj_id,
        p.name,
        p.status,
        p.go_live,
        -- Generate Salesforce-style ID for this Project record
        CONCAT('00P', SUBSTRING(MD5(TRIM(p.proj_id)), 1, 13)) AS project_id,
        a.account_id,
        o.opportunity_id
    FROM proj_raw p
    LEFT JOIN accounts a
        ON TRIM(p.kd) = a.kunden_key
    LEFT JOIN opportunities o
        ON TRIM(p.opp) = o.chance_key
)

SELECT
    -- Primary key: deterministic Salesforce-style ID from source proj_id
    CAST(project_id AS TEXT) AS "Id",

    -- Project name; default to empty string for NOT NULL compliance
    COALESCE(TRIM(name), '') AS "Name",

    -- Status mapped to target enum domain (Active, Completed, In Planning, On Hold, Cancelled)
    CASE LOWER(TRIM(status))
        WHEN 'active'       THEN 'Active'
        WHEN 'completed'    THEN 'Completed'
        WHEN 'in planning'  THEN 'In Planning'
        WHEN 'on hold'      THEN 'On Hold'
        WHEN 'cancelled'    THEN 'Cancelled'
        -- Common German source values
        WHEN 'aktiv'        THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'geplant'      THEN 'In Planning'
        WHEN 'pausiert'     THEN 'On Hold'
        WHEN 'storniert'    THEN 'Cancelled'
        WHEN 'in bearbeitung' THEN 'In Planning'
        WHEN 'angehalten'   THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",

    -- Go-Live date: parse common European formats into ISO YYYY-MM-DD
    CASE
        WHEN go_live IS NOT NULL AND TRIM(go_live) != '' THEN
            CASE
                -- DD.MM.YYYY (common German format)
                WHEN go_live ~ '^\d{2}\.\d{2}\.\d{4}$'
                    THEN TO_CHAR(TO_DATE(TRIM(go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD')
                -- YYYY-MM-DD already ISO
                WHEN go_live ~ '^\d{4}-\d{2}-\d{2}$'
                    THEN TRIM(go_live)
                -- MM/DD/YYYY (US format)
                WHEN go_live ~ '^\d{2}/\d{2}/\d{4}$'
                    THEN TO_CHAR(TO_DATE(TRIM(go_live), 'MM/DD/YYYY'), 'YYYY-MM-DD')
                -- DD/MM/YYYY
                WHEN go_live ~ '^\d{2}-\d{2}-\d{4}$'
                    THEN TO_CHAR(TO_DATE(TRIM(go_live), 'DD-MM-YYYY'), 'YYYY-MM-DD')
                ELSE NULL  -- unparseable → NULL per guidelines
            END
        ELSE NULL
    END AS "Go_Live_Date__c",

    -- Account reference: Salesforce-style Account Id (looked up from source kd key)
    CAST(account_id AS TEXT) AS "Account__c",

    -- Opportunity reference: Salesforce-style Opportunity Id (looked up from source opp key)
    CAST(opportunity_id AS TEXT) AS "Opportunity__c",

    -- Legacy key for row-level verification
    TRIM(proj_id) AS "Legacy_Project_ID__c",

    -- Derived audit fields (no source equivalents)
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM projects
WHERE proj_id IS NOT NULL AND TRIM(proj_id) != '';