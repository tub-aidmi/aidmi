{{ config(materialized='table') }}

SELECT
    o.id AS "Id",
    o.name AS "Name",
    CASE
        WHEN o.stage = 'Prospecting' THEN 'Prospecting'
        WHEN o.stage = 'Qualification' THEN 'Qualification'
        WHEN o.stage = 'Closed Won' THEN 'Closed Won'
        WHEN o.stage = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL StageName if source contains unexpected values
    END AS "StageName",
    TO_CHAR(
        CASE
            WHEN project_dates.min_go_live ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(project_dates.min_go_live, 'YYYY-MM-DD')
            ELSE TO_DATE('9999-12-31', 'YYYY-MM-DD') -- Default date for NOT NULL
        END, 'YYYY-MM-DD'
    ) AS "CloseDate",
    o.amount AS "Amount",
    'USD' AS "CurrencyIsoCode",
    a.id AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'opportunity') }} AS o
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON SUBSTRING(o.customer_number FROM '[0-9]+') = SUBSTRING(a.id FROM '[0-9]+')
LEFT JOIN (
    SELECT
        opportunity_ref,
        MIN(go_live) AS min_go_live
    FROM
        {{ source('fixture_missing_relations_v2_src', 'project') }}
    GROUP BY
        opportunity_ref
) AS project_dates
ON o.id = project_dates.opportunity_ref