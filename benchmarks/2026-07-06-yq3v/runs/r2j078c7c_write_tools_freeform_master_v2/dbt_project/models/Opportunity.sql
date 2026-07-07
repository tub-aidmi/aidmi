{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        opp_kennung,
        titel,
        vertriebsphase,
        zieldatum,
        auftragswert,
        waehrungscode,
        kunden_ref
    FROM {{ source('fixture_master_v2_src', 'master_opportunities') }}
)
SELECT
    MD5(opp_kennung) AS "Id",
    COALESCE(titel, 'Unknown Opportunity') AS "Name",
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
        ELSE 'Prospecting' -- Fallback for NOT NULL target
    END AS "StageName",
    CASE
        WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{2}\/\d{2}\/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE TO_CHAR(NOW(), 'YYYY-MM-DD') -- Fallback for NOT NULL
    END AS "CloseDate",
    NULLIF(
        CASE
            WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
            ELSE
                REPLACE(
                    REPLACE(
                        REGEXP_REPLACE(auftragswert, '[^0-9,.]', '', 'g'),
                        '.', ''
                    ),
                    ',', '.'
                )
        END, ''
    )::DOUBLE PRECISION AS "Amount",
    waehrungscode AS "CurrencyIsoCode",
    MD5(kunden_ref) AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM source_data
