{{ config(materialized='table') }}""",
"""
SELECT
    o.id AS "Id",
    COALESCE(o.name, 'Untitled Opportunity') AS "Name",
    CASE
        WHEN o.stage = 'Prospecting' THEN 'Prospecting'
        WHEN o.stage = 'Qualification' THEN 'Qualification'
        WHEN o.stage = 'Closed Won' THEN 'Closed Won'
        WHEN o.stage = 'Closed Lost' THEN 'Closed Lost'
        -- Default for NOT NULL target and unmapped source values
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE
            -- YYYY-MM-DD format
            WHEN p.go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(CAST(p.go_live AS DATE), 'YYYY-MM-DD')
            -- MM/DD/YYYY format
            WHEN p.go_live ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            -- DD.MM.YYYY format
            WHEN p.go_live ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE NULL -- If none of the recognized date formats match, return NULL from CASE
        END,
        '1900-01-01' -- Fallback for NOT NULL target if go_live is NULL or unparseable, using a deterministic date
    ) AS "CloseDate",
    o.amount AS "Amount",
    NULL AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON o.customer_number = a.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS p
    ON o.id = p.opportunity_ref
"""))