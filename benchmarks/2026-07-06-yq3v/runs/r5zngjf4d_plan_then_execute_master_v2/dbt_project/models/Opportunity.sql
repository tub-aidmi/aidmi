-- dbt model for Opportunity

{{ config(materialized='table') }}

SELECT
    MD5(o.opp_kennung)::TEXT AS "Id",
    COALESCE(TRIM(o.titel), 'Unknown Opportunity Name') AS "Name",
    CASE
        WHEN TRIM(LOWER(o.vertriebsphase)) = 'prospecting' THEN 'Prospecting'
        WHEN TRIM(LOWER(o.vertriebsphase)) = 'qualification' THEN 'Qualification'
        WHEN TRIM(LOWER(o.vertriebsphase)) = 'needs analysis' THEN 'Needs Analysis'
        WHEN TRIM(LOWER(o.vertriebsphase)) = 'value proposition' THEN 'Value Proposition'
        WHEN TRIM(LOWER(o.vertriebsphase)) = 'id. decision makers' THEN 'Id. Decision Makers'
        WHEN TRIM(LOWER(o.vertriebsphase)) = 'perception analysis' THEN 'Perception Analysis'
        WHEN TRIM(LOWER(o.vertriebsphase)) = 'proposal/price quote' THEN 'Proposal/Price Quote'
        WHEN TRIM(LOWER(o.vertriebsphase)) = 'negotiation/review' THEN 'Negotiation/Review'
        WHEN TRIM(LOWER(o.vertriebsphase)) = 'closed won' THEN 'Closed Won'
        WHEN TRIM(LOWER(o.vertriebsphase)) = 'closed lost' THEN 'Closed Lost'
        ELSE 'Prospecting' -- Default for NOT NULL target
    END AS "StageName",
    COALESCE(
        CASE
            WHEN o.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN o.zieldatum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
            WHEN o.zieldatum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(o.zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
            ELSE NULL
        END,
        '1900-01-01' -- Default for NOT NULL target
    ) AS "CloseDate",
    NULLIF(
        TRIM(
            REPLACE(
                REPLACE(
                    REGEXP_REPLACE(o.auftragswert, '[€$]', '', 'g'),
                '.', '', 'g'),
            ',', '.')
        ), '')::DOUBLE PRECISION AS "Amount",
    o.waehrungscode AS "CurrencyIsoCode",
    MD5(k.kundennummer)::TEXT AS "AccountId",
    o.opp_kennung AS "Legacy_Opportunity_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
    ON o.kunden_ref = k.kundennummer