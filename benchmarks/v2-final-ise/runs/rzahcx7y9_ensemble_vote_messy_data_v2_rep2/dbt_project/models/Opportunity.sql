{{ config(materialized='table') }}

SELECT
    CAST(o.id AS TEXT) AS "Id",
    CASE WHEN TRIM(o.name) = '' THEN NULL ELSE INITCAP(TRIM(o.name)) END AS "Name",
    -- StageName: map source values to enum domain with normalization
    CASE
        WHEN LOWER(TRIM(o.stagename)) IN ('prospecting') THEN 'Prospecting'
        WHEN LOWER(TRIM(o.stagename)) IN ('qualifcation', 'qualification') THEN 'Qualification'
        WHEN LOWER(TRIM(o.stagename)) IN ('needs analysis', 'need analysis', 'needsanalysis') THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o.stagename)) IN ('value proposition', 'valueproposition') THEN 'Value Proposition'
        WHEN LOWER(TRIM(o.stagename)) IN ('identify decision makers', 'identifying decision makers', 'id. decision makers') THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o.stagename)) IN ('perception analysis', 'perceptionanalysis') THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o.stagename)) IN ('proposal/price quote', 'proposal price quote', 'proposal/pricequote', 'proposal &amp; price quote') THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o.stagename)) IN ('negotiation/review', 'negotiation review', 'negotiation &amp; review') THEN 'Negotiation/Review'
        WHEN LOWER(TRIM(o.stagename)) IN ('closed won', 'won') THEN 'Closed Won'
        WHEN LOWER(TRIM(o.stagename)) IN ('closed lost', 'lost') THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    -- CloseDate: parse multiple formats, output ISO YYYY-MM-DD text
    CASE
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(o.closedate, 'YYYY-MM-DD')::TEXT
        WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(o.closedate, 'DD.MM.YYYY')::TEXT
        WHEN o.closedate ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(o.closedate, 'MM/DD/YYYY')::TEXT
        WHEN o.closedate ~ '^\d{8}$' THEN TO_DATE(o.closedate, 'YYYYMMDD')::TEXT
        ELSE NULL
    END AS "CloseDate",
    -- Amount: clean symbols, handle European dot+comma format, cast to double precision. Return NULL for non-numeric values.
    CASE
        WHEN o.amount IS NULL OR TRIM(o.amount) = '' THEN NULL
        ELSE
            CAST(
                CASE
                    WHEN REGEXP_REPLACE(o.amount, '[^\d.,\-]', '', 'g') = '' THEN NULL
                    -- European format detection: digits-dot-3digits-comma-digits (e.g. 1.234,56)
                    WHEN REGEXP_REPLACE(o.amount, '[^\d.,\-]', '', 'g') ~ '\.\d{3},\d+$' THEN
                        REPLACE(REPLACE(REGEXP_REPLACE(o.amount, '[^\d.,\-]', '', 'g'), '.', ''), ',', '.')::DOUBLE PRECISION
                    ELSE REGEXP_REPLACE(o.amount, '[^\d.,\-]', '', 'g')::DOUBLE PRECISION
                END
            AS DOUBLE PRECISION)
    END AS "Amount",
    UPPER(TRIM(COALESCE(o.currencyisocode, ''))) AS "CurrencyIsoCode",
    -- AccountId: join to source Account table to get Salesforce-style Id (prefix '001')
    CASE WHEN a.id IS NOT NULL THEN a.id ELSE o.accountid END AS "AccountId",
    CAST(o.id AS TEXT) AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON o.accountid = a.erp_number__c OR o.accountid = a.id