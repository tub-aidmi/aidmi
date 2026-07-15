{{ config(materialized='table') }}

WITH source_proj AS (
    SELECT
        proj_id,
        name,
        status,
        go_live,
        kd,
        opp
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
),

normalised AS (
    SELECT
        proj_id,
        INITCAP(TRIM(name)) AS name,
        status,
        go_live,
        TRIM(UPPER(REGEXP_REPLACE(kd, '^[^A-Z0-9]+', '', 'g'))) AS norm_kd_key,
        TRIM(UPPER(REGEXP_REPLACE(COALESCE(opp, ''), '^[^A-Z0-9]+', '', 'g'))) AS norm_opp_key
    FROM source_proj
),

date_parsed AS (
    SELECT
        n.*,
        CASE
            WHEN n.go_live IS NULL OR TRIM(n.go_live) = '' THEN NULL
            WHEN n.go_live ~ '^\d{2}\.\d{2}\.\d{4}$'
                THEN TO_DATE(TRIM(n.go_live), 'DD.MM.YYYY')::TEXT
            WHEN n.go_live ~ '^\d{8}$' OR n.go_live ~ '^\d{2}\.\d{2}\.\d{4}[\s]\d{2}:\d{2}'
                THEN TO_DATE(SUBSTRING(TRIM(n.go_live) FROM 1 FOR 8), 'YYYYMMDD')::TEXT
            WHEN n.go_live ~ '^\d{2}/\d{2}/\d{4}$'
                THEN TO_DATE(TRIM(n.go_live), 'MM/DD/YYYY')::TEXT
            WHEN n.go_live ~ '^\d{4}-\d{2}-\d{2}'
                THEN SUBSTRING(TRIM(n.go_live) FROM 1 FOR 10)
            ELSE NULL
        END AS go_live_iso
    FROM normalised n
),

account_lookup AS (
    SELECT
        TRIM(UPPER(REGEXP_REPLACE(k.kunden_nr, '^[^A-Z0-9]+', '', 'g'))) AS norm_kunden_key,
        CONCAT('006', SUBSTR(MD5(TRIM(UPPER(REGEXP_REPLACE(k.kunden_nr, '^[^A-Z0-9]+', '', 'g')))), 1, 12)) AS account_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
),

opportunity_lookup AS (
    SELECT
        TRIM(UPPER(REGEXP_REPLACE(c.chance_id, '^[^A-Z0-9]+', '', 'g'))) AS norm_chance_key,
        CONCAT('006', SUBSTR(MD5(TRIM(UPPER(REGEXP_REPLACE(c.chance_id, '^[^A-Z0-9]+', '', 'g')))), 1, 12)) AS opportunity_id
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
),

status_mapped AS (
    SELECT
        dp.*,
        CASE LOWER(TRIM(COALESCE(dp.status, '')))
            WHEN 'aktiv' THEN 'Active'
            WHEN 'abgeschlossen' THEN 'Completed'
            WHEN 'in planung' THEN 'In Planning'
            WHEN 'auf hold' THEN 'On Hold'
            WHEN 'gekündigt' THEN 'Cancelled'
            WHEN 'storniert' THEN 'Cancelled'
            ELSE NULL
        END AS project_status_mapped
    FROM date_parsed dp
),

joined AS (
    SELECT
        sm.*,
        al.account_id AS account_sf_id,
        ol.opportunity_id AS opportunity_sf_id
    FROM status_mapped sm
    LEFT JOIN account_lookup al ON al.norm_kunden_key = sm.norm_kd_key
    LEFT JOIN opportunity_lookup ol ON ol.norm_chance_key = sm.norm_opp_key
),

with_proj_id AS (
    SELECT
        j.*,
        CONCAT('006', SUBSTR(MD5(TRIM(UPPER(REGEXP_REPLACE(j.proj_id, '^[^A-Z0-9]+', '', 'g')))), 1, 12)) AS proj_sf_id
    FROM joined j
)

SELECT
    proj_sf_id AS "Id",
    name AS "Name",
    project_status_mapped AS "Project_Status__c",
    go_live_iso AS "Go_Live_Date__c",
    account_sf_id AS "Account__c",
    opportunity_sf_id AS "Opportunity__c",
    TRIM(proj_id) AS "Legacy_Project_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.US') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.US') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM with_proj_id