{{ config(materialized='table') }}

SELECT
    MD5(chancen.chance_id)::UUID::TEXT AS "Id",
    COALESCE(TRIM(INITCAP(chancen.bezeichnung)), 'Unknown Opportunity') AS "Name",
    CASE LOWER(TRIM(chancen.phase))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE 'Qualification'
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(chancen.abschlussdatum, 'YYYY-MM-DD')
                WHEN chancen.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(chancen.abschlussdatum, 'DD.MM.YYYY')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        '1900-01-01'
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    TRIM(UPPER(chancen.waehrung)) AS "CurrencyIsoCode",
    MD5(kunden.kunden_nr)::UUID::TEXT AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
ON
    chancen.kd_nr = kunden.kunden_nr
