{{ config(materialized='table') }}

WITH cleaned_opportunities AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref,
        -- Defaulting CreatedDate and LastModifiedDate as source doesn't provide
        CAST(CURRENT_TIMESTAMP AS TEXT) AS created_date,
        CAST(CURRENT_TIMESTAMP AS TEXT) AS last_modified_date
    FROM
        {{ source('fixture_master_v2_src', 'master_opportunities') }}
)
SELECT
    MD5(opp_kennung) AS "Id",
    COALESCE(titel, 'Unknown Opportunity') AS "Name", -- Name is NOT NULL
    CASE
        WHEN LOWER(vertriebsphase) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(vertriebsphase) = 'qualification' THEN 'Qualification'
        WHEN LOWER(vertriebsphase) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(vertriebsphase) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(vertriebsphase) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(vertriebsphase) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(vertriebsphase) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(vertriebsphase) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(vertriebsphase) = 'closed won' THEN 'Closed Won'
        WHEN LOWER(vertriebsphase) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to 'Prospecting' for NOT NULL
    END AS "StageName",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(zieldatum, 'YYYY-MM-DD')
                WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(zieldatum, 'DD.MM.YYYY')
                WHEN zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(zieldatum, 'MM/DD/YYYY')
                ELSE NULL
            END,
        'YYYY-MM-DD'),
    '1900-01-01') AS "CloseDate", -- Default for NOT NULL
    CASE
        WHEN auftragswert ~ '^[0-9]+([.,][0-9]+)?$' THEN
            CAST(REPLACE(REPLACE(auftragswert, '.', ''), ',', '.') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    waehrungscode AS "CurrencyIsoCode",
    MD5(kunden_ref) AS "AccountId", -- AccountId is derived from kunden_ref
    opp_kennung AS "Legacy_Opportunity_ID__c",
    created_date AS "CreatedDate",
    last_modified_date AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    cleaned_opportunities
