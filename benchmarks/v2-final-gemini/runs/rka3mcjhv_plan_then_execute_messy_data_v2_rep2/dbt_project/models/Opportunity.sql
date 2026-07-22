-- depends_on: {{ ref('fixture_messy_data_v2_src', 'opportunity') }}

{{ config(materialized='table') }}

SELECT
    opportunity.id AS "Id",
    COALESCE(TRIM(INITCAP(opportunity.name)), 'Unknown Opportunity') AS "Name",
    COALESCE(
        CASE
            WHEN TRIM(INITCAP(opportunity.stagename)) IN ('Prospecting', 'Qualification', 'Needs Analysis', 'Value Proposition', 'Id. Decision Makers', 'Perception Analysis', 'Proposal/Price Quote', 'Negotiation/Review', 'Closed Won', 'Closed Lost')
            THEN TRIM(INITCAP(opportunity.stagename))
            ELSE NULL
        END,
        'Prospecting'
    ) AS "StageName",
    COALESCE(
        CASE
            WHEN opportunity.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN opportunity.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN opportunity.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(opportunity.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01'
    ) AS "CloseDate",
    CASE
        WHEN (TRIM(opportunity.amount) = '' OR opportunity.amount IS NULL) THEN NULL
        ELSE
            CASE
                WHEN (TRIM(
                          REGEXP_REPLACE(
                              CASE
                                  WHEN opportunity.amount LIKE '%.%,' AND POSITION(',' IN opportunity.amount) > POSITION('.' IN opportunity.amount) THEN
                                      REPLACE(REPLACE(opportunity.amount, '.', ''), ',', '.')
                                  WHEN opportunity.amount LIKE '%,' AND POSITION('.' IN opportunity.amount) = 0 THEN
                                      REPLACE(opportunity.amount, ',', '.')
                                  WHEN opportunity.amount LIKE '%,.%' AND POSITION('.' IN opportunity.amount) > POSITION(',' IN opportunity.amount) THEN
                                      REPLACE(opportunity.amount, ',', '')
                                  ELSE
                                      opportunity.amount
                              END,
                              '[^0-9.-]', '', 'g'
                          )
                      )
                     ) ~ '^-?\d+(\.\d+)?$'
                THEN CAST(TRIM(
                             REGEXP_REPLACE(
                                 CASE
                                     WHEN opportunity.amount LIKE '%.%,' AND POSITION(',' IN opportunity.amount) > POSITION('.' IN opportunity.amount) THEN
                                         REPLACE(REPLACE(opportunity.amount, '.', ''), ',', '.')
                                     WHEN opportunity.amount LIKE '%,' AND POSITION('.' IN opportunity.amount) = 0 THEN
                                         REPLACE(opportunity.amount, ',', '.')
                                     WHEN opportunity.amount LIKE '%,.%' AND POSITION('.' IN opportunity.amount) > POSITION(',' IN opportunity.amount) THEN
                                         REPLACE(opportunity.amount, ',', '')
                                     ELSE
                                         opportunity.amount
                                 END,
                                 '[^0-9.-]', '', 'g'
                             )
                         ) AS DOUBLE PRECISION)
                ELSE NULL
            END
    END AS "Amount",
    TRIM(UPPER(opportunity.currencyisocode)) AS "CurrencyIsoCode",
    opportunity.accountid AS "AccountId",
    opportunity.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS opportunity