 accompanies {{ config(materialized='table') }}

SELECT
    o."Id" AS "Id",
    COALESCE(o."Name", 'Unknown Opportunity') AS "Name",
    CASE
        WHEN LOWER(TRIM(o."StageName")) IN ('closed won', 'won', 'gewonnen', 'abgeschlossen (gewonnen)') THEN 'Closed Won'
        WHEN LOWER(TRIM(o."StageName")) IN ('closed lost', 'lost', 'verloren', 'abgeschlossen (verloren)') THEN 'Closed Lost'
        WHEN LOWER(TRIM(o."StageName")) IN ('qualification', 'qualifikation', 'quali') THEN 'Qualification'
        WHEN LOWER(TRIM(o."StageName")) = 'prospecting' THEN 'Prospecting'
        WHEN LOWER(TRIM(o."StageName")) = 'needs analysis' THEN 'Needs Analysis'
        WHEN LOWER(TRIM(o."StageName")) = 'value proposition' THEN 'Value Proposition'
        WHEN LOWER(TRIM(o."StageName")) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN LOWER(TRIM(o."StageName")) = 'perception analysis' THEN 'Perception Analysis'
        WHEN LOWER(TRIM(o."StageName")) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN LOWER(TRIM(o."StageName")) = 'negotiation/review' THEN 'Negotiation/Review'
        ELSE 'Prospecting' -- Default for unmapped or unknown stages
    END AS "StageName",
    TO_CHAR(
        COALESCE(
            (CASE
                WHEN o."CloseDate" ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(o."CloseDate" AS DATE)
                WHEN o."CloseDate" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(o."CloseDate", 'MM/DD/YYYY')
                WHEN o."CloseDate" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(o."CloseDate", 'DD.MM.YYYY')
                WHEN o."CloseDate" ~ '^\d{8}$' THEN TO_DATE(o."CloseDate", 'YYYYMMDD')
                ELSE NULL
            END),
            '1900-01-01'::DATE
        ),
        'YYYY-MM-DD'
    ) AS "CloseDate",
    CASE
        WHEN REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(TRIM(o."Amount"), '[^0-9\.\,\-]', '', 'g'),
                    '\.(?=\d+,)', '', 'g'
                ),
                ',' ,'.' ,'g'
             ) ~ '^-?\d*\.?\d+$'
        THEN CAST(REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(TRIM(o."Amount"), '[^0-9\.\,\-]', '', 'g'),
                    '\.(?=\d+,)', '', 'g'
                ),
                ',' ,'.' ,'g'
             ) AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    o."CurrencyIsoCode" AS "CurrencyIsoCode",
    o."AccountId" AS "AccountId",
    NULL AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Opportunity') }} AS o