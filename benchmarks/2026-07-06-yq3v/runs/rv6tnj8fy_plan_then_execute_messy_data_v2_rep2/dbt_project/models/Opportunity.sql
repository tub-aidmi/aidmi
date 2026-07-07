-- dbt model for Opportunity

{{ config(materialized='table') }}

SELECT
    TRIM(opportunity.id) AS "Id",
    COALESCE(TRIM(opportunity.name), 'Unknown Opportunity') AS "Name",
    COALESCE(
        CASE UPPER(TRIM(opportunity.stagename))
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
    COALESCE(
        (CASE WHEN TRIM(opportunity.closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'YYYY-MM-DD'), 'YYYY-MM-DD') ELSE NULL END),
        (CASE WHEN TRIM(opportunity.closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD') ELSE NULL END),
        (CASE WHEN TRIM(opportunity.closedate) ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD') ELSE NULL END),
        CURRENT_DATE::TEXT
    ) AS "CloseDate",
    NULLIF(
        CASE
            WHEN REGEXP_REPLACE(opportunity.amount, '[^0-9\.,]', '', 'g') ~ '^\d+\.\d{3},\d+$' THEN -- European (e.g. 1.234,56)
                REPLACE(REPLACE(REGEXP_REPLACE(opportunity.amount, '[^0-9\.,]', '', 'g'), '.', ''), ',', '.')
            WHEN REGEXP_REPLACE(opportunity.amount, '[^0-9\.,]', '', 'g') ~ '^\d{1,3}(,\d{3})*\.\d+$' THEN -- US (e.g. 1,234.56)
                REPLACE(REGEXP_REPLACE(opportunity.amount, '[^0-9\.,]', '', 'g'), ',', '')
            WHEN REGEXP_REPLACE(opportunity.amount, '[^0-9\.,]', '', 'g') ~ '^\d+,\d+$' THEN -- European simple (e.g. 1234,56)
                REPLACE(REGEXP_REPLACE(opportunity.amount, '[^0-9\.,]', '', 'g'), ',', '.')
            WHEN REGEXP_REPLACE(opportunity.amount, '[^0-9\.,]', '', 'g') ~ '^\d+(\.\d+)?$' THEN -- US simple (e.g. 1234.56) or integer
                REGEXP_REPLACE(opportunity.amount, '[^0-9\.,]', '', 'g')
            ELSE NULL -- Unparseable
        END,
        ''
    )::DOUBLE PRECISION AS "Amount",
    TRIM(opportunity.currencyisocode) AS "CurrencyIsoCode",
    TRIM(opportunity.accountid) AS "AccountId",
    TRIM(opportunity.id) AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opportunity