{{ config(materialized='table') }}

SELECT
    opp_kennung AS "Id",
    titel AS "Name",
    CASE
        WHEN UPPER(TRIM(vertriebsphase)) = 'PROSPECTING' THEN 'Prospecting'
        WHEN UPPER(TRIM(vertriebsphase)) = 'QUALIFICATION' THEN 'Qualification'
        WHEN UPPER(TRIM(vertriebsphase)) = 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN UPPER(TRIM(vertriebsphase)) = 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN UPPER(TRIM(vertriebsphase)) = 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN UPPER(TRIM(vertriebsphase)) = 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN UPPER(TRIM(vertriebsphase)) = 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN UPPER(TRIM(vertriebsphase)) = 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN UPPER(TRIM(vertriebsphase)) = 'CLOSED WON' THEN 'Closed Won'
        WHEN UPPER(TRIM(vertriebsphase)) = 'CLOSED LOST' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN zieldatum
            WHEN zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END, '1900-01-01' -- Default for NOT NULL
    ) AS "CloseDate",
    CASE
        WHEN auftragswert IS NULL OR TRIM(auftragswert) = '' THEN NULL
        ELSE
            NULLIF(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REPLACE(auftragswert, '.', ''), -- Remove thousand separators for European style
                        ',', '.' -- Replace comma with dot for decimal
                    ),
                    '[^0-9.]', '', 'g' -- Remove any other non-numeric chars except dot
                ),
                ''
            )::DOUBLE PRECISION
    END AS "Amount",
    waehrungscode AS "CurrencyIsoCode",
    kunden_ref AS "AccountId",
    opp_kennung AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_src', 'master_opportunities') }}
