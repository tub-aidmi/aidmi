{{ config(materialized='table') }}

SELECT
    TRIM(opportunity.id) AS "Id",
    COALESCE(TRIM(opportunity.name), 'Unnamed Opportunity ' || TRIM(opportunity.id)) AS "Name",
    CASE
        WHEN LOWER(TRIM(opportunity.stagename)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
        WHEN LOWER(TRIM(opportunity.stagename)) IN ('qualification', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(opportunity.stagename)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(opportunity.stagename)) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(opportunity.stagename)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(opportunity.stagename)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(opportunity.stagename)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(opportunity.stagename)) IN ('negotiation/review', 'in prüfung') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(opportunity.stagename)) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(opportunity.stagename)) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        ELSE 'Prospecting' -- Fallback for NOT NULL target
    END AS "StageName",
    CASE
        WHEN TRIM(opportunity.closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN TRIM(opportunity.closedate) ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(opportunity.closedate) ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN TRIM(opportunity.closedate) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(opportunity.closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN TRIM(REGEXP_REPLACE(opportunity.amount, '[^0-9\.,-]', '', 'g')) ~ '^-?([0-9]{1,3}(\.[0-9]{3})*|[0-9]+),([0-9]{1,2})$' THEN
            REPLACE(REPLACE(TRIM(REGEXP_REPLACE(opportunity.amount, '[^0-9\.,-]', '', 'g')), '.', ''), ',', '.')::DOUBLE PRECISION
        WHEN TRIM(REGEXP_REPLACE(opportunity.amount, '[^0-9\.-]', '', 'g')) ~ '^-?[0-9]+(\.[0-9]{1,2})?$' THEN
            TRIM(REGEXP_REPLACE(opportunity.amount, '[^0-9\.-]', '', 'g'))::DOUBLE PRECISION
        ELSE NULL
    END AS "Amount",
    TRIM(opportunity.currencyisocode) AS "CurrencyIsoCode",
    TRIM(opportunity.accountid) AS "AccountId",
    TRIM(opportunity.id) AS "Legacy_Opportunity_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opportunity
WHERE
    opportunity.id IS NOT NULL
