{{ config(materialized='table') }}

SELECT
    id AS "Id",
    COALESCE(INITCAP(TRIM(name)), 'Unknown Opportunity') AS "Name",
    CASE LOWER(TRIM(stagename))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'identification of decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE
        WHEN closedate IS NULL OR TRIM(closedate) = '' THEN NULL
        WHEN REGEXP_REPLACE(TRIM(closedate), '[^0-9]', '', 'g') ~ '^\d{8}$' THEN
            SUBSTR(REGEXP_REPLACE(TRIM(closedate), '[^0-9]', '', 'g'), 1, 4) || '-' ||
            SUBSTR(REGEXP_REPLACE(TRIM(closedate), '[^0-9]', '', 'g'), 5, 2) || '-' ||
            SUBSTR(REGEXP_REPLACE(TRIM(closedate), '[^0-9]', '', 'g'), 7, 2)
        WHEN TRIM(closedate) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
            TO_CHAR(TO_DATE(TRIM(closedate), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN
            TO_CHAR(TO_DATE(TRIM(closedate), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN TRIM(closedate) ~ '^\d{4}-\d{2}-\d{2}$' THEN
            TRIM(closedate)
        ELSE NULL
    END AS "CloseDate",
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        ELSE
            CAST(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REPLACE(TRIM(REGEXP_REPLACE(amount, '[A-Z]{3}', '', 'gi')), '.', ''),
                    ',', '.'),
                '[^0-9.]', '', 'g')
            AS DOUBLE PRECISION)
    END AS "Amount",
    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    accountid AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}