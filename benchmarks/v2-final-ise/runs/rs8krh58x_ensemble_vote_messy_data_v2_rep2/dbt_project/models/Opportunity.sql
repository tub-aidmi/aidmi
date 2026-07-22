{{ config(materialized='table') }}

WITH clean_amount AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        currencyisocode,
        accountid,
        -- Strip currency symbols and whitespace; handle European format
        CAST(
            CASE 
                -- European dot+comma: remove thousands-sep dots first, then replace comma with decimal point
                WHEN REGEXP_REPLACE(amount, '[^\d.,\-+]', '') ~ '^\-?\d+\.\d{3},\d+$' THEN
                    REGEXP_REPLACE(REGEXP_REPLACE(amount, '[^\d.,\-+]', ''), '\.', '')
                -- General: remove non-numeric except dot/comma/minus/+
                ELSE REGEXP_REPLACE(amount, '[^\d.,\-+]', '')
            END
            , ',') AS DOUBLE PRECISION)  AS amount_val
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
)

SELECT
    id::TEXT AS "Id",
    CASE WHEN name IS NULL OR TRIM(name) = '' THEN '' ELSE INITCAP(TRIM(name)) END AS "Name",
    CASE 
        WHEN LOWER(TRIM(stagename)) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(stagename)) IN ('qualifcation', 'qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(stagename)) IN ('needs analysis', 'need analysis', 'needsanalysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'valueproposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(stagename)) IN ('identify decision makers', 'identifying decision makers', 'id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(stagename)) IN ('perception analysis', 'perceptionanalysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal price quote', 'proposal/pricequote', 'proposal &amp; price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation review', 'negotiation/review', 'negotiation &amp; review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(stagename)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        -- ISO format YYYY-MM-DD
        WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(closedate, 'YYYY-MM-DD')::TEXT
        -- European dot separator DD.MM.YYYY  
        WHEN closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(closedate, 'DD.MM.YYYY')::TEXT
        -- US slash format MM/DD/YYYY
        WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(closedate, 'MM/DD/YYYY')::TEXT
        -- Compact YYYYMMDD
        WHEN closedate ~ '^\d{8}$' THEN TO_DATE(closedate, 'YYYYMMDD')::TEXT
        -- DD/MM/YYYY (European with slashes, day part <= 12 is ambiguous but common)
        WHEN closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(closedate, 'DD/MM/YYYY')::TEXT
        ELSE NULL
    END AS "CloseDate",
    amount_val::DOUBLE PRECISION AS "Amount",
    UPPER(TRIM(currencyisocode)) AS "CurrencyIsoCode",
    accountid::TEXT AS "AccountId",
    id::TEXT AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"

FROM clean_amount
WHERE amount_val IS NOT NULL