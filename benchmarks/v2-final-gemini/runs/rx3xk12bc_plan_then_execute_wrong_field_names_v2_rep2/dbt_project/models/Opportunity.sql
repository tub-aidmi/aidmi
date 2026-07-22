{{ config(materialized='table') }}

SELECT
    MD5(chancen.chance_id::TEXT) AS "Id",
    TRIM(chancen.bezeichnung) AS "Name",
    COALESCE(
        CASE UPPER(TRIM(chancen.phase))
            WHEN 'PROSPECTING' THEN 'Prospecting'
            WHEN 'QUALIFICATION' THEN 'Qualification'
            WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
            WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
            WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
            WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
            WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
            WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
            WHEN 'CLOSED WON' THEN 'Closed Won'
            WHEN 'CLOSED LOST' THEN 'Closed Lost'
            ELSE NULL
        END,
        'Prospecting'
    ) AS "StageName",
    TO_CHAR(
        COALESCE(
            CASE
                WHEN chancen.abschlussdatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(chancen.abschlussdatum, 'YYYY-MM-DD')
                WHEN chancen.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(chancen.abschlussdatum, 'DD.MM.YYYY')
                WHEN chancen.abschlussdatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(chancen.abschlussdatum, 'MM/DD/YYYY')
                ELSE NULL
            END,
            CURRENT_DATE
        ),
        'YYYY-MM-DD'
    ) AS "CloseDate",
    chancen.volumen AS "Amount",
    COALESCE(UPPER(TRIM(chancen.waehrung)), 'USD') AS "CurrencyIsoCode",
    MD5(kunden.kunden_nr::TEXT) AS "AccountId",
    chancen.chance_id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} AS chancen
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON chancen.kd_nr = kunden.kunden_nr