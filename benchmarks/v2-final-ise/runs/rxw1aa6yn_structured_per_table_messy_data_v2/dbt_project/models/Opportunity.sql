{{ config(materialized='table') }}

SELECT 
    o.id AS "Id",
    COALESCE(NULLIF(o.name, ''), 'Unnamed Opportunity') AS "Name",
    CASE 
        WHEN o.stagename ~* 'prospect' THEN 'Prospecting'
        WHEN o.stagename ~* 'qualif' THEN 'Qualification'
        WHEN o.stagename ~* 'needs' THEN 'Needs Analysis'
        WHEN o.stagename ~* 'value' THEN 'Value Proposition'
        WHEN o.stagename ~* 'id\.?\s*decision' THEN 'Id. Decision Makers'
        WHEN o.stagename ~* 'perception' THEN 'Perception Analysis'
        WHEN o.stagename ~* 'proposal' THEN 'Proposal/Price Quote'
        WHEN o.stagename ~* 'negotiation' THEN 'Negotiation/Review'
        WHEN o.stagename ~* 'closed\s*won' THEN 'Closed Won'
        WHEN o.stagename ~* 'closed\s*lost' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    CASE 
        WHEN o.closedate ~ '^\d{4}-\d{2}-\d{2}$' THEN o.closedate
        WHEN o.closedate ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.closedate, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN o.closedate ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.closedate, 'YYYYMMDD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "CloseDate",
    CASE 
        WHEN o.amount ~ '^[\d\s]+[.,]\d{2}$' THEN 
            CAST(REGEXP_REPLACE(REGEXP_REPLACE(o.amount, '[\s\.]', '', 'g'), ',', '.', 'g') AS DOUBLE PRECISION)
        WHEN o.amount ~ '^\d+$' THEN CAST(o.amount AS DOUBLE PRECISION)
        WHEN o.amount ~ '^[\d\s]+$' THEN CAST(REGEXP_REPLACE(o.amount, '[\s]', '', 'g') AS DOUBLE PRECISION)
        ELSE NULL
    END AS "Amount",
    NULLIF(o.currencyisocode, '') AS "CurrencyIsoCode",
    o.accountid AS "AccountId",
    o.id AS "Legacy_Opportunity_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'opportunity') }} o