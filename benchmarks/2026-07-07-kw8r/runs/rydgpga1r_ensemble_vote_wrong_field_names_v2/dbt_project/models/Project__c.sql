{{ config(materialized='table') }}

WITH accounts AS (
    SELECT
        kunden_nr,
        CONCAT('001', SUBSTRING(MD5(TRIM(kunden_nr)), 1, 13)) AS sf_account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
    WHERE kunden_nr IS NOT NULL
      AND TRIM(kunden_nr) != ''
),

opportunities AS (
    SELECT
        chance_id,
        CONCAT('006', SUBSTRING(MD5(TRIM(chance_id)), 1, 13)) AS sf_opportunity_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
    WHERE chance_id IS NOT NULL
      AND TRIM(chance_id) != ''
),

projects AS (
    SELECT
        p.proj_id,
        p.name,
        p.status,
        p.go_live,
        a.sf_account_id,
        o.sf_opportunity_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
    LEFT JOIN accounts a
        ON TRIM(p.kd) = TRIM(a.kunden_nr)
    LEFT JOIN opportunities o
        ON TRIM(p.opp) = TRIM(o.chance_id)
)

SELECT
    CONCAT('00P', SUBSTRING(MD5(TRIM(proj_id)), 1, 13)) AS "Id",
    COALESCE(TRIM(name), '') AS "Name",
    CASE LOWER(TRIM(status))
        WHEN 'active'         THEN 'Active'
        WHEN 'completed'      THEN 'Completed'
        WHEN 'in planning'    THEN 'In Planning'
        WHEN 'on hold'        THEN 'On Hold'
        WHEN 'cancelled'      THEN 'Cancelled'
        WHEN 'aktiv'          THEN 'Active'
        WHEN 'abgeschlossen'  THEN 'Completed'
        WHEN 'geplant'        THEN 'In Planning'
        WHEN 'pausiert'       THEN 'On Hold'
        WHEN 'storniert'      THEN 'Cancelled'
        WHEN 'in bearbeitung' THEN 'In Planning'
        WHEN 'angehalten'     THEN 'On Hold'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN go_live IS NOT NULL AND TRIM(go_live) != '' THEN
            CASE
                WHEN TRIM(go_live) ~ '^\d{2}\.\d{2}\.\d{4}$'
                    THEN TO_CHAR(TO_DATE(TRIM(go_live), 'DD.MM.YYYY'), 'YYYY-MM-DD')
                WHEN TRIM(go_live) ~ '^\d{4}-\d{2}-\d{2}$'
                    THEN TRIM(go_live)
                WHEN TRIM(go_live) ~ '^\d{2}/\d{2}/\d{4}$'
                    THEN TO_CHAR(TO_DATE(TRIM(go_live), 'MM/DD/YYYY'), 'YYYY-MM-DD')
                ELSE NULL
            END
        ELSE NULL
    END AS "Go_Live_Date__c",
    CAST(sf_account_id AS TEXT) AS "Account__c",
    CAST(sf_opportunity_id AS TEXT) AS "Opportunity__c",
    TRIM(proj_id) AS "Legacy_Project_ID__c",
    CAST(CURRENT_DATE AS TEXT) AS "CreatedDate",
    CAST(CURRENT_DATE AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM projects
WHERE proj_id IS NOT NULL
  AND TRIM(proj_id) != '';