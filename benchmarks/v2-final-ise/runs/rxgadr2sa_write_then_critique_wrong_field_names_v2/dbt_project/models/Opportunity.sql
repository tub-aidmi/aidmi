{{ config(materialized='table') }}

WITH chancen_base AS (
    SELECT
        '006' || TRIM(c.chance_id) AS "Id",
        INITCAP(TRIM(c.bezeichnung)) AS "Name",
        CASE LOWER(TRIM(c.phase))
            WHEN 'prospektierung' THEN 'Prospecting'
            WHEN 'qualifikation'  THEN 'Qualification'
            WHEN 'bedarfsanalyse' THEN 'Needs Analysis'
            WHEN 'value proposition' THEN 'Value Proposition'
            WHEN 'entscheidungsfinder identifizieren' THEN 'Id. Decision Makers'
            WHEN 'wahrnehmungsanalyse' THEN 'Perception Analysis'
            WHEN 'angebot/preisangebot' THEN 'Proposal/Price Quote'
            WHEN 'verhandlung/review'   THEN 'Negotiation/Review'
            WHEN 'gewonnen'             THEN 'Closed Won'
            WHEN 'verloren'             THEN 'Closed Lost'
            ELSE NULL
        END AS "StageName",
        CASE
            WHEN TRIM(c.abschlussdatum) IS NOT NULL AND TRIM(c.abschlussdatum) != '' THEN
                CASE
                    WHEN c.abschlussdatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(TRIM(c.abschlussdatum), 'DD.MM.YYYY')::TEXT
                    WHEN c.abschlussdatum ~ '^\d{8}$'             THEN TO_DATE(TRIM(c.abschlussdatum), 'YYYYMMDD')::TEXT
                    ELSE NULL
                END
            ELSE NULL
        END AS "CloseDate",
        CAST(c.volumen AS DOUBLE PRECISION) AS "Amount",
        INITCAP(TRIM(c.waehrung)) AS "CurrencyIsoCode",
        c.chance_id AS "Legacy_Opportunity_ID__c",
        c.kd_nr AS "_kd_nr"
    FROM {{ source('fixture_wrong_field_names_v2_src', 'chancen') }} c
),
kunden_mapping AS (
    SELECT
        '001' || TRIM(k.kunden_nr) AS account_id,
        k.kunden_nr
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
)

SELECT
    cb."Id",
    COALESCE(cb."Name", '') AS "Name",
    COALESCE(cb."StageName", 'Prospecting') AS "StageName",
    COALESCE(cb."CloseDate", NULL) AS "CloseDate",
    cb."Amount",
    cb."CurrencyIsoCode",
    km.account_id AS "AccountId",
    cb."Legacy_Opportunity_ID__c",
    CAST(NOW() AS TEXT) AS "CreatedDate",
    CAST(NOW() AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM chancen_base cb
LEFT JOIN kunden_mapping km
    ON TRIM(cb._kd_nr) = TRIM(km.kunden_nr)