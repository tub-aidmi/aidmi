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
        TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'YYYY-MM-DD'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD'),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "CloseDate",
    CASE
        WHEN TRIM(opportunity.amount) ~ '^[0-9]+(\\.[0-9]{3})*,[0-9]+$' THEN
            CAST(REPLACE(REPLACE(TRIM(opportunity.amount), '.', ''), ',', '.') AS DOUBLE PRECISION)
        WHEN TRIM(opportunity.amount) ~ '^[0-9]+,[0-9]+$' THEN
            CAST(REPLACE(TRIM(opportunity.amount), ',', '.') AS DOUBLE PRECISION)
        WHEN TRIM(opportunity.amount) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN
            CAST(TRIM(opportunity.amount) AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    UPPER(TRIM(opportunity.currencyisocode)) AS "CurrencyIsoCode",
    TRIM(opportunity.accountid) AS "AccountId",
    TRIM(opportunity.id) AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opportunity
