{{ config(materialized='table') }}

SELECT
    CAST(chance_id AS text) AS "Id",
    coalesce(trim(bezeichnung), 'Unknown') AS "Name",
    case
        when upper(trim(phase)) = 'CLOSED WON' then 'Closed Won'
        when upper(trim(phase)) = 'CLOSED LOST' then 'Closed Lost'
        when upper(trim(phase)) = 'PROSPECTING' then 'Prospecting'
        when upper(trim(phase)) = 'QUALIFICATION' then 'Qualification'
        else 'Prospecting'
    end as "StageName",
    coalesce(
        CASE
            WHEN abschlussdatum IS NOT NULL AND abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$'
                THEN TO_CHAR(TO_DATE(abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN abschlussdatum IS NOT NULL AND abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$'
                THEN TO_CHAR(TO_DATE(abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        CURRENT_DATE::TEXT
    ) AS "CloseDate",
    volumen AS "Amount",
    waehrung AS "CurrencyIsoCode",
    kd_nr AS "AccountId",
    chance_id AS "Legacy_Opportunity_ID__c",
    CAST(NULL AS text) AS "CreatedDate",
    CAST(NULL AS text) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
