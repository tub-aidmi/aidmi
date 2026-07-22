{{ config(materialized='table') }}

SELECT
    MD5(o.opp_kennung) AS "Id",
    COALESCE(TRIM(o.titel), 'Unknown Opportunity') AS "Name",
    COALESCE(
        CASE LOWER(TRIM(o.vertriebsphase))
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
            ELSE NULL
        END, 'Prospecting') AS "StageName",
    COALESCE(
        CASE
            WHEN TRIM(o.zieldatum) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(o.zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN TRIM(o.zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(o.zieldatum), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN TRIM(o.zieldatum) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(o.zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01'
    ) AS "CloseDate",
    NULLIF(
        REPLACE(
            REPLACE(
                REGEXP_REPLACE(TRIM(o.auftragswert), '[^0-9,\.]', '', 'g'),
                '.', ''
            ),
            ',', '.'
        ),
        ''
    )::DOUBLE PRECISION AS "Amount",
    TRIM(o.waehrungscode) AS "CurrencyIsoCode",
    MD5(o.kunden_ref) AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
