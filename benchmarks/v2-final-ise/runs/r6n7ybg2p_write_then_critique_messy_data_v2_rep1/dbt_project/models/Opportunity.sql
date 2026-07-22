{{ config(materialized='table') }}

WITH cleaned_stage AS (
    SELECT
        op.id,
        op.name,
        op.stagename,
        op.closedate,
        op.amount,
        op.currencyisocode,
        TRIM(op.accountid) AS raw_accountid,
        acc.id AS account_id_sfdc,
        CASE LOWER(TRIM(COALESCE(op.stagename, '')))
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
        END AS stage_name_cleaned,
        CASE
            WHEN op.closedate IS NULL OR TRIM(op.closedate) = '' THEN NULL
            WHEN op.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(op.closedate, 'YYYY-MM-DD')::TEXT
            WHEN op.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(op.closedate, 'MM/DD/YYYY')::TEXT
            WHEN op.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(op.closedate, 'DD.MM.YYYY')::TEXT
            WHEN op.closedate ~ '^\d{8}$' AND
                 SUBSTR(op.closedate, 5, 2) BETWEEN '01' AND '12' AND
                 SUBSTR(op.closedate, 7, 2) BETWEEN '01' AND '31' THEN TO_DATE(op.closedate, 'YYYYMMDD')::TEXT
            ELSE NULL
        END AS close_date_cleaned
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} op
    LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} acc
        ON TRIM(op.accountid) = TRIM(acc.legacy_customer_id__c)
)

SELECT
    id AS "Id",
    COALESCE(TRIM(name), 'Unknown') AS "Name",
    stage_name_cleaned AS "StageName",
    close_date_cleaned AS "CloseDate",
    CASE
        WHEN amount IS NULL OR TRIM(amount) = '' THEN NULL
        ELSE
            CASE 
                -- European format: comma followed by exactly 2 digits at end (e.g., "1.234,56")
                WHEN REGEXP_REPLACE(amount, '[^0-9.,\-]', '', 'g') ~ ',\d{2}$' THEN
                    CAST(REGEXP_REPLACE(
                        REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^0-9.,\-]', '', 'g'), '\.', ''), 
                        ',', '.') AS DOUBLE PRECISION)
                -- US format with decimal dot: ends with dot + 1-2 digits (e.g., "1234.56" or "1,234.56")
                WHEN REGEXP_REPLACE(amount, '[^0-9.,\-]', '', 'g') ~ '\.\d{1,2}$' THEN
                    CAST(REGEXP_REPLACE(
                        REGEXP_REPLACE(amount, '[^0-9.,\-]', '', 'g'), ',', '') AS DOUBLE PRECISION)
                ELSE 
                    -- No recognisable decimal separator pattern; strip to digits/minus/dot only
                    CAST(REGEXP_REPLACE(amount, '[^0-9.\-]', '', 'g') AS DOUBLE PRECISION)
            END
    END AS "Amount",
    UPPER(TRIM(COALESCE(currencyisocode, ''))) AS "CurrencyIsoCode",
    account_id_sfdc AS "AccountId",
    id AS "Legacy_Opportunity_ID__c",
    CAST(NULL AS TEXT) AS "CreatedDate",
    CAST(NULL AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM cleaned_stage