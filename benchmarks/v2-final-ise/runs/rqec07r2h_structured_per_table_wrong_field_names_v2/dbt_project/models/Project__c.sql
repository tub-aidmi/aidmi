{{ config(materialized='table') }}

SELECT
    CAST(p.proj_id AS TEXT) AS "Id",
    CASE WHEN TRIM(p.name) = '' THEN NULL ELSE INITCAP(TRIM(p.name)) END AS "Name",
    CASE LOWER(TRIM(COALESCE(p.status, '')))
        WHEN 'aktiv'   THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planung'  THEN 'In Planning'
        WHEN 'angehalten'  THEN 'On Hold'
        WHEN 'storniert'   THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live IS NOT NULL AND TRIM(p.go_live) != ''
             AND p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$'
        THEN TO_DATE(TRIM(p.go_live), 'DD.MM.YYYY')::TEXT
        ELSE NULL
    END AS "Go_Live_Date__c",
    CASE
        WHEN k.erp_nummer IS NOT NULL AND TRIM(k.erp_nummer) != ''
            THEN TRIM(k.erp_nummer)
        ELSE CAST('A-' || REGEXP_REPLACE(TRIM(k.kunden_nr), '[^0-9]', '', 'g') AS TEXT)
    END AS "Account__c",
    CASE
        WHEN c.chance_id IS NOT NULL AND TRIM(c.chance_id) != ''
            THEN
                CASE
                    WHEN TRIM(c.chance_id) ~ '^006' THEN TRIM(c.chance_id)
                    ELSE 'O-' || REGEXP_REPLACE(TRIM(c.chance_id), '[^0-9]', '', 'g')
                END
        ELSE NULL
    END AS "Opportunity__c",
    CAST(p.proj_id AS TEXT) AS "Legacy_Project_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }} p
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(p.kd) = TRIM(k.kunden_nr)
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
    ON TRIM(p.opp) = TRIM(c.chance_id)
WHERE p.proj_id IS NOT NULL