-- models/Opportunity.sql
{{ config(materialized='table') }}

SELECT
    MD5(opps.opp_kennung) AS "Id",
    COALESCE(opps.titel, opps.opp_kennung) AS "Name",
    CASE
        WHEN opps.vertriebsphase = 'Prospecting' THEN 'Prospecting'
        WHEN opps.vertriebsphase = 'Qualification' THEN 'Qualification'
        WHEN opps.vertriebsphase = 'Needs Analysis' THEN 'Needs Analysis'
        WHEN opps.vertriebsphase = 'Value Proposition' THEN 'Value Proposition'
        WHEN opps.vertriebsphase = 'Id. Decision Makers' THEN 'Id. Decision Makers'
        WHEN opps.vertriebsphase = 'Perception Analysis' THEN 'Perception Analysis'
        WHEN opps.vertriebsphase = 'Proposal/Price Quote' THEN 'Proposal/Price Quote'
        WHEN opps.vertriebsphase = 'Negotiation/Review' THEN 'Negotiation/Review'
        WHEN opps.vertriebsphase = 'Closed Won' THEN 'Closed Won'
        WHEN opps.vertriebsphase = 'Closed Lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    COALESCE(
        (CASE
            WHEN opps.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_DATE(opps.zieldatum, 'DD.MM.YYYY')
            WHEN opps.zieldatum ~ '^\d{8}$' THEN TO_DATE(opps.zieldatum, 'YYYYMMDD')
            WHEN opps.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(opps.zieldatum, 'MM/DD/YYYY')
            ELSE NULL
        END)::text,
        CURRENT_DATE::text -- Fallback for NOT NULL target CloseDate
    ) AS "CloseDate",
    CASE
        WHEN opps.auftragswert IS NULL THEN NULL
        WHEN REGEXP_REPLACE(opps.auftragswert, '[€$, ]', '') ~ '^[0-9]+([,.][0-9]+)?$' THEN
            CASE
                WHEN opps.auftragswert LIKE '%.%' AND opps.auftragswert LIKE '%,%' THEN
                    REPLACE(REPLACE(opps.auftragswert, '.', ''), ',', '.')::DOUBLE PRECISION
                WHEN opps.auftragswert LIKE '%,%' THEN
                    REPLACE(opps.auftragswert, ',', '.')::DOUBLE PRECISION
                ELSE
                    REGEXP_REPLACE(opps.auftragswert, '[^0-9.]', '')::DOUBLE PRECISION
            END
        ELSE NULL
    END AS "Amount",
    opps.waehrungscode AS "CurrencyIsoCode",
    MD5(opps.kunden_ref) AS "AccountId",
    opps.opp_kennung AS "Legacy_Opportunity_ID__c",
    NOW()::text AS "CreatedDate",
    NOW()::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS opps
