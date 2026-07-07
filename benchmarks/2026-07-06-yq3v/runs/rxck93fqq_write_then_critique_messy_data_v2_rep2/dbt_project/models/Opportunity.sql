-- {{ config(materialized='table') }}

WITH raw_opportunity AS (
    SELECT
        id,
        name,
        stagename,
        closedate,
        amount,
        currencyisocode,
        accountid
    FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }}
),
cleaned_data AS (
    SELECT
        id,
        COALESCE(TRIM(name), 'Unnamed Opportunity') AS name,
        
        CASE
            WHEN LOWER(TRIM(stagename)) IN ('prospecting', 'prospect', 'in kontakt') THEN 'Prospecting'
            WHEN LOWER(TRIM(stagename)) IN ('qualification', 'quali', 'qualifikation', 'in prufung') THEN 'Qualification'
            WHEN LOWER(TRIM(stagename)) IN ('needs analysis', 'needs_analysis') THEN 'Needs Analysis'
            WHEN LOWER(TRIM(stagename)) IN ('value proposition', 'value_proposition') THEN 'Value Proposition'
            WHEN REGEXP_REPLACE(LOWER(TRIM(stagename)), '[^a-z0-9]', '') IN ('iddecisionmakers', 'iddecisionmakers', 'identificationdecisionmakers') THEN 'Id. Decision Makers'
            WHEN LOWER(TRIM(stagename)) IN ('perception analysis', 'perception_analysis') THEN 'Perception Analysis'
            WHEN LOWER(TRIM(stagename)) IN ('proposal/price quote', 'proposal_price_quote', 'proposal / price quote') THEN 'Proposal/Price Quote'
            WHEN LOWER(TRIM(stagename)) IN ('negotiation/review', 'negotiation_review', 'negotiation / review') THEN 'Negotiation/Review'
            WHEN LOWER(TRIM(stagename)) IN ('closed won', 'closed_won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
            WHEN LOWER(TRIM(stagename)) IN ('closed lost', 'closed_lost', 'lost', 'verloren') THEN 'Closed Lost'
            ELSE 'Prospecting' -- Default value for StageName as it is NOT NULL
        END AS stagename,

        -- Handle multiple date formats for CloseDate using regex guards, prefer NULL for unparseable
        COALESCE(
            CASE WHEN closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYY-MM-DD'), 'YYYY-MM-DD') END,
            CASE WHEN closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD') END,
            CASE WHEN closedate ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_CHAR(TO_DATE(closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD') END,
            CASE WHEN closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(closedate, 'YYYYMMDD'), 'YYYY-MM-DD') END
        ) AS closedate_parsed,

        -- Clean and convert Amount to double precision, handling European and US formats
        TRIM(REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(amount, '[$€£¥CHF CAD AUD]', '', 'g'), -- Remove common currency symbols
                '[[:space:]]', '', 'g' -- Remove all whitespace
            ),
            '[^0-9.,-]', '', 'g' -- Remove any remaining non-numeric, non-dot, non-comma, non-hyphen chars
        )) AS cleaned_amount_str,
        
        currencyisocode,
        accountid,
        id AS legacy_opportunity_id__c,
        NULL AS createddate,
        NULL AS lastmodifieddate,
        0 AS isdeleted
    FROM raw_opportunity
)
SELECT
    id AS "Id",
    name AS "Name",
    stagename AS "StageName",
    -- CloseDate is TEXT NOT NULL in target schema; defaulting to '1900-01-01' for unparseable/missing dates.
    COALESCE(closedate_parsed, '1900-01-01') AS "CloseDate",
    CASE
        WHEN cleaned_amount_str IS NULL OR TRIM(cleaned_amount_str) = '' THEN NULL
        -- European format: dot as thousand separator, comma as decimal (e.g., 1.234,56)
        WHEN cleaned_amount_str ~ '^-?\d{1,3}(\.\d{3})*,\d+$' THEN
            CAST(REPLACE(REPLACE(cleaned_amount_str, '.', ''), ',', '.') AS DOUBLE PRECISION)
        -- US/Standard format: comma as thousand separator, dot as decimal (e.g., 1,234.56 or 1234.56)
        WHEN cleaned_amount_str ~ '^-?\d{1,3}(,\d{3})*\.\d+$' OR cleaned_amount_str ~ '^-?\d+$' OR cleaned_amount_str ~ '^-?\d+\.\d+$' THEN
            CAST(REPLACE(cleaned_amount_str, ',', '') AS DOUBLE PRECISION)
        ELSE NULL -- Unparseable after cleaning
    END AS "Amount",
    currencyisocode AS "CurrencyIsoCode",
    accountid AS "AccountId",
    legacy_opportunity_id__c AS "Legacy_Opportunity_ID__c",
    createddate AS "CreatedDate",
    lastmodifieddate AS "LastModifiedDate",
    isdeleted AS "IsDeleted"
FROM cleaned_data;