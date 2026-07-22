{{ config(materialized='table') }}

SELECT
    opportunity.id AS "Id",
    COALESCE(opportunity.name, 'Unnamed Opportunity') AS "Name",
    CASE
        WHEN LOWER(opportunity.stagename) IN ('prospecting', 'prospect') THEN 'Prospecting'
        WHEN LOWER(opportunity.stagename) IN ('qualification', 'quali', 'in prüfung') THEN 'Qualification'
        WHEN LOWER(opportunity.stagename) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(opportunity.stagename) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(opportunity.stagename) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(opportunity.stagename) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(opportunity.stagename) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(opportunity.stagename) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN LOWER(opportunity.stagename) IN ('closed won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(opportunity.stagename) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default to Prospecting if unmapped and NOT NULL
    END AS "StageName",
    COALESCE(
        CASE
            WHEN opportunity.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'YYYYMMDD'), 'YYYY-MM-DD') -- YYYYMMDD
            WHEN opportunity.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD') -- YYYY-MM-DD
            WHEN opportunity.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD') -- MM/DD/YYYY
            WHEN opportunity.closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD') -- DD.MM.YYYY
            ELSE '1900-01-01' -- Default value for unparseable dates to satisfy NOT NULL
        END,
        '1900-01-01' -- Default if source is NULL
    ) AS "CloseDate",
    NULLIF(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    LOWER(TRIM(opportunity.amount)),
                    '[^0-9,.-]+', '', 'g'
                ),
                '\.(?=\d{3}(?:,|$))', '', 'g'
            ),
            ',', '.', 'g'
        ),
        ''
    )::DOUBLE PRECISION AS "Amount",
    CASE LOWER(opportunity.currencyisocode)
        WHEN 'euro' THEN 'EUR'
        WHEN 'eur' THEN 'EUR'
        WHEN '€' THEN 'EUR'
        WHEN 'dollar' THEN 'USD'
        WHEN 'usd' THEN 'USD'
        WHEN '$' THEN 'USD'
        WHEN 'chf' THEN 'CHF'
        WHEN 'gbp' THEN 'GBP'
        WHEN '£' THEN 'GBP'
        ELSE NULL
    END AS "CurrencyIsoCode",
    opportunity.accountid AS "AccountId",
    opportunity.id AS "Legacy_Opportunity_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opportunity
