{{ config(materialized='table') }}

WITH proj AS (
  SELECT *
  FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
),
kunden AS (
  SELECT kunden_nr, erp_nummer
  FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),
chancen AS (
  SELECT chance_id
  FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
)

SELECT
    CAST(p.proj_id AS TEXT) AS "Id",
    CASE WHEN TRIM(p.name) = '' THEN NULL ELSE INITCAP(TRIM(p.name)) END AS "Name",
    CASE LOWER(TRIM(COALESCE(p.status, '')))
        WHEN 'aktiv'   THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planung'  THEN 'In Planning'
        WHEN 'angehalten'  THEN 'On Hold'
        WHEN 'storniert'   THEN 'Cancelled'
        WHEN 'active'      THEN 'Active'
        WHEN 'completed'   THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'on hold'     THEN 'On Hold'
        WHEN 'cancelled'   THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live IS NOT NULL AND TRIM(p.go_live) != ''
        THEN
            -- Try DD.MM.YYYY format first (German standard), then MM/DD/YYYY, then YYYY-MM-DD
            CASE
                WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$'
                    THEN TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY')::TEXT
                WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$'
                    THEN TO_DATE(TRIM(p.go_live), 'MM/DD/YYYY')::TEXT
                WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$'
                    THEN TO_DATE(TRIM(p.go_live), 'YYYY-MM-DD')::TEXT
                ELSE NULL
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    -- Map proj.kd (customer number) to Salesforce-style Account ID via kunden.erp_nummer
    CASE
        WHEN c.erp_nummer IS NOT NULL AND TRIM(c.erp_nummer) != ''
        THEN TRIM(c.erp_nummer)
        WHEN c.kunden_nr IS NOT NULL AND TRIM(c.kunden_nr) != ''
        THEN 'A-' || REGEXP_REPLACE(TRIM(c.kunden_nr), '[^0-9]', '', 'g')
        ELSE NULL
    END AS "Account__c",
    -- Map proj.opp to Salesforce-style Opportunity ID via chancen.chance_id
    CASE
        WHEN co.chance_id IS NOT NULL AND TRIM(co.chance_id) != ''
        THEN COALESCE(NULLIF(TRIM(SUBSTRING(co.chance_id FROM '^006[A-Za-z0-9]{12,15}$')), ''), 'O-' || REGEXP_REPLACE(TRIM(co.chance_id), '[^0-9]', '', 'g'))
        ELSE NULL
    END AS "Opportunity__c",
    -- Legacy key for row-level verification
    CAST(p.proj_id AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM proj p
LEFT JOIN kunden c
    ON TRIM(p.kd) = TRIM(c.kunden_nr)
LEFT JOIN chancen co
    ON TRIM(COALESCE(p.opp, '')) = TRIM(COALESCE(co.chance_id, ''))
WHERE p.proj_id IS NOT NULL;