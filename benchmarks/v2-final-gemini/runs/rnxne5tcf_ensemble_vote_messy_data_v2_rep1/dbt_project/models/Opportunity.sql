{{ config(materialized='table') }}

SELECT
    source.id AS "Id",
    COALESCE(source.name, 'Unnamed Opportunity') AS "Name",
    CASE LOWER(TRIM(source.stagename))
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'lost' THEN 'Closed Lost'
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'prospect' THEN 'Prospecting'
        WHEN 'qualifikation' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification'
        WHEN 'in prüfung' THEN 'Needs Analysis'
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for NOT NULL constraint
    END AS "StageName",
    COALESCE(
        CASE
            WHEN source.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(source.closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN source.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(source.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN source.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(source.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN source.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(source.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') -- Default for NOT NULL constraint
    ) AS "CloseDate",
    CASE
        WHEN TRIM(source.amount) IS NULL OR TRIM(source.amount) = '' THEN NULL
        ELSE
            CASE
                WHEN POSITION(',' IN TRIM(source.amount)) > 0 THEN -- Contains comma, assume European decimal separator
                    (REPLACE(REPLACE(REGEXP_REPLACE(TRIM(source.amount), '[^0-9\.\,\-]+', '', 'g'), '.', '', 'g'), ',', '.'))::DOUBLE PRECISION
                ELSE -- No comma, assume US decimal or integer
                    (REPLACE(REGEXP_REPLACE(TRIM(source.amount), '[^0-9\.\,\-]+', '', 'g'), ',', '', 'g'))::DOUBLE PRECISION
            END
    END AS "Amount",
    UPPER(source.currencyisocode) AS "CurrencyIsoCode",
    source.accountid AS "AccountId",
    source.id AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} AS source
