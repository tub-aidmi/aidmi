-- depends_on: {{ ref("Account") }}
{{ config(materialized='table') }}

SELECT
    MD5(TRIM(c.chance_id)) AS "Id",
    COALESCE(TRIM(c.bezeichnung), 'Unknown Opportunity') AS "Name",
    CASE
        WHEN TRIM(LOWER(c.phase)) = 'prospecting' THEN 'Prospecting'
        WHEN TRIM(LOWER(c.phase)) = 'qualification' THEN 'Qualification'
        WHEN TRIM(LOWER(c.phase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN TRIM(LOWER(c.phase)) = 'value proposition' THEN 'Value Proposition'
        WHEN TRIM(LOWER(c.phase)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(c.phase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN TRIM(LOWER(c.phase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(c.phase)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(c.phase)) = 'closed won' THEN 'Closed Won'
        WHEN TRIM(LOWER(c.phase)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Unknown Stage' -- Fallback for NOT NULL target and unmapped enum
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN TRIM(c.abschlussdatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(TRIM(c.abschlussdatum), 'YYYY-MM-DD')
                WHEN TRIM(c.abschlussdatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(c.abschlussdatum), 'DD.MM.YYYY')
                WHEN TRIM(c.abschlussdatum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(TRIM(c.abschlussdatum), 'MM/DD/YYYY')
                WHEN TRIM(c.abschlussdatum) ~ '^\d{8}$' THEN TO_DATE(TRIM(c.abschlussdatum), 'YYYYMMDD')
                ELSE NULL
            END
        , 'YYYY-MM-DD'),
        '1900-01-01'
    ) AS "CloseDate",
    c.volumen::DOUBLE PRECISION AS "Amount",
    TRIM(c.waehrung) AS "CurrencyIsoCode",
    MD5(TRIM(k.kunden_nr)) AS "AccountId",
    TRIM(c.chance_id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS c
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    TRIM(c.kd_nr) = TRIM(k.kunden_nr)