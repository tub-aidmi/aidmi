-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'chancen') }}
-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}

{{ config(materialized='table') }}

SELECT
    o.chance_id AS "Id",
    COALESCE(TRIM(o.bezeichnung), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN UPPER(TRIM(o.phase)) = 'PROSPEKTIERUNG' THEN 'Prospecting'
        WHEN UPPER(TRIM(o.phase)) = 'QUALIFIZIERUNG' THEN 'Qualification'
        WHEN UPPER(TRIM(o.phase)) = 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(o.phase)) = 'WERTANGEBOT' THEN 'Value Proposition'
        WHEN UPPER(TRIM(o.phase)) = 'ID. ENTSCHEIDER' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(o.phase)) = 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(o.phase)) = 'ANGEBOT/PREISANGEBOT' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(o.phase)) = 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(o.phase)) = 'GEWONNEN' THEN 'Closed Won'
        WHEN UPPER(TRIM(o.phase)) = 'VERLOREN' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE WHEN o.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(o.abschlussdatum, 'YYYY-MM-DD'), 'YYYY-MM-DD') ELSE NULL END,
        CASE WHEN o.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.abschlussdatum, 'DD.MM.YYYY'), 'YYYY-MM-DD') ELSE NULL END,
        CASE WHEN o.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.abschlussdatum, 'MM/DD/YYYY'), 'YYYY-MM-DD') ELSE NULL END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    o.volumen AS "Amount",
    o.waehrung AS "CurrencyIsoCode",
    k.kunden_nr AS "AccountId",
    o.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS o
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    o.kd_nr = k.kunden_nr