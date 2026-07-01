
{{ config(materialized='table') }}

SELECT
    chance.chance_id AS "Id",
    COALESCE(chance.bezeichnung, 'Opportunity ' || chance.chance_id) AS "Name",
    CASE chance.phase
        WHEN 'Prospecting' THEN 'Prospecting'
        WHEN 'Qualification' THEN 'Qualification'
        WHEN 'Closed Lost' THEN 'Closed Lost'
        WHEN 'Closed Won' THEN 'Closed Won'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN chance.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(chance.abschlussdatum, 'YYYY-MM-DD')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    chance.volumen AS "Amount",
    chance.waehrung AS "CurrencyIsoCode",
    chance.kd_nr AS "AccountId",
    chance.chance_id AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chance
