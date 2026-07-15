{{ config(materialized='table') }}

with opp_raw as (
    select
        {{ source('fixture_master_v2_src', 'master_opportunities') }}.*
    from {{ source('fixture_master_v2_src', 'master_opportunities') }}
),
account_raw as (
    select * from {{ source('fixture_master_v2_src', 'master_kunden') }}
),
stg_opportunity as (
    select
        -- Generate Salesforce-style ID (006 prefix for Opportunity)
        '006' || LPAD(CAST(op.opp_kennung AS INTEGER), 8, '0') AS "Id",
        
        op.titel AS "Name",

        -- Map German Sales Stages to Standard Enum
        CASE UPPER(TRIM(op.vertriebsphase))
            WHEN 'LEAD' THEN 'Prospecting'
            WHEN 'INTERESSENT' THEN 'Prospecting'
            WHEN 'QUALIFIZIERUNG' THEN 'Qualification'
            WHEN 'BEDARFSANALYSE' THEN 'Needs Analysis'
            WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
            WHEN 'WERTPROPOSITION' THEN 'Value Proposition'
            WHEN 'ENTSCHEIDUNGSTRAEGER' THEN 'Id. Decision Makers'
            WHEN 'PERZEPTIONSANALYSE' THEN 'Perception Analysis'
            WHEN 'ANGEBOT' THEN 'Proposal/Price Quote'
            WHEN 'ANGEBOTSPHASE' THEN 'Proposal/Price Quote'
            WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
            WHEN 'VERHANDLUNG' THEN 'Negotiation/Review'
            WHEN 'NEGOZIATION' THEN 'Negotiation/Review'
            WHEN 'CLOSED WON' THEN 'Closed Won'
            WHEN 'GEWONNEN' THEN 'Closed Won'
            WHEN 'ABGESCHLOSSEN (GEWONNEN)' THEN 'Closed Won'
            WHEN 'CLOSED LOST' THEN 'Closed Lost'
            WHEN 'VERLOREN' THEN 'Closed Lost'
            WHEN 'ABGESCHLOSSEN (VERLOREN)' THEN 'Closed Lost'
            ELSE NULL
        END AS "StageName",

        -- Parse CloseDate (Zieldatum) - Handles DD.MM.YYYY and YYYY-MM-DD
        CASE 
            WHEN op.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(op.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN op.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN op.zieldatum -- Already YYYY-MM-DD
            ELSE NULL
        END AS "CloseDate",

        -- Parse Amount - Handle European formatting (e.g. 1.234,56)
        CASE 
            WHEN op.auftragswert IS NOT NULL AND op.auftragswert ~ '^\-?\d{1,3}(\.\d{3})*,\d+$' 
                THEN REGEXP_REPLACE(op.auftragswert, '[.]', '')::VARCHAR || '.' || SPLIT_PART(REGEXP_REPLACE(op.auftragswert, '[.]', ''), ',', -1)::DOUBLE PRECISION
            WHEN op.auftragswert IS NOT NULL AND op.auftragswert ~ '^\-?\d+,\d+$' 
                THEN REGEXP_REPLACE(op.auftragswert, ',', '.')::DOUBLE PRECISION
            WHEN op.auftragswert IS NOT NULL AND op.auftragswert ~ '^\-?\d+(\.\d+)?$'
                THEN op.auftragswert::DOUBLE PRECISION
            ELSE NULL
        END AS "Amount",

        -- Currency
        op.waehrungscode AS "CurrencyIsoCode",

        -- AccountId (Join with Accounts to get SFDC-style ID)
        '001' || LPAD(CAST(ak.kundennummer AS INTEGER), 8, '0') AS "AccountId",

        -- Legacy Keys
        op.opp_kennung AS "Legacy_Opportunity_ID__c"

    from opp_raw op
    left join account_raw ak
        on TRIM(op.kunden_ref) = TRIM(ak.kundennummer)
)
select
    *
from stg_opportunity
where "Id" is not null