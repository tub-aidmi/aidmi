
{{ config(materialized='table') }}

SELECT
    op."Id" AS "Id",
    COALESCE(op."Name", 'Unknown') AS "Name",
    CASE LOWER(TRIM(op."StageName"))
        WHEN 'prospecting' THEN 'Prospecting'
        WHEN 'prospect' THEN 'Prospecting'
        WHEN 'in kontakt' THEN 'Prospecting'
        WHEN 'qualification' THEN 'Qualification'
        WHEN 'quali' THEN 'Qualification гибкий план' THEN 'Qualification' -- Assuming this is a typo and should be Qualification
        WHEN 'needs analysis' THEN 'Needs Analysis'
        WHEN 'value proposition' THEN 'Value Proposition'
        WHEN 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN 'perception analysis' THEN 'Perception Analysis'
        WHEN 'in prüfung' THEN 'Perception Analysis'
        WHEN 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN 'negotiation/review' THEN 'Negotiation/Review'
        WHEN 'closed won' THEN 'Closed Won'
        WHEN 'won' THEN 'Closed Won'
        WHEN 'gewonnen' THEN 'Closed Won'
        WHEN 'abgeschlossen (gewonnen)' THEN 'Closed Won'
        WHEN 'closed lost' THEN 'Closed Lost'
        WHEN 'lost' THEN 'Closed Lost'
        WHEN 'verloren' THEN 'Closed Lost'
        WHEN 'abgeschlossen (verloren)' THEN 'Closed Lost'
        ELSE 'Prospecting'
    END AS "StageName",
    COALESCE(
        CASE
            WHEN op."CloseDate" ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(op."CloseDate" AS DATE)
            WHEN op."CloseDate" ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(op."CloseDate", 'MM/DD/YYYY')
            WHEN op."CloseDate" ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' THEN TO_DATE(op."CloseDate", 'DD.MM.YYYY')
            WHEN op."CloseDate" ~ '^\d{8}$' THEN TO_DATE(op."CloseDate", 'YYYYMMDD')
            ELSE NULL
        END,
        '1900-01-01'::DATE
    )::TEXT AS "CloseDate",
    CAST(
        CASE
            WHEN op."Amount" IS NULL THEN NULL
            ELSE
                CASE
                    WHEN REGEXP_REPLACE(TRIM(LEADING 'EUR ' FROM REGEXP_REPLACE(op."Amount", ' ', '', 'g')), '[0-9]', '', 'g') LIKE '%,%' THEN
                        -- Contains a comma, assume European format: remove dots, then replace comma with dot
                        REPLACE(REPLACE(TRIM(LEADING 'EUR ' FROM REGEXP_REPLACE(op."Amount", ' ', '', 'g')), '.', ''), ',', '.')
                    ELSE
                        -- No comma, assume standard format
                        TRIM(LEADING 'EUR ' FROM REGEXP_REPLACE(op."Amount", ' ', '', 'g'))
                END
        END AS DOUBLE PRECISION
    ) AS "Amount",
    op."CurrencyIsoCode" AS "CurrencyIsoCode",
    op."AccountId" AS "AccountId",
    NULL::TEXT AS "Legacy_Opportunity_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_src', 'Opportunity') }} AS op
