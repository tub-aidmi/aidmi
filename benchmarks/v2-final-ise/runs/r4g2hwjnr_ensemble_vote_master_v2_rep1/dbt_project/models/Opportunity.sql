{{ config(materialized='table') }}

with opp_raw as (
    select * from {{ source('fixture_master_v2_src', 'master_opportunities') }}
),
account_raw as (
    select * from {{ source('fixture_master_v2_src', 'master_kunden') }}
),
stg_opportunity as (
    select
        -- Generate Salesforce-style ID (006 prefix for Opportunity)
        '006' || LPAD(CAST(op.opp_kennung AS VARCHAR), 8, '0') AS "Id",

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
            ELSE 'Prospecting'
        END AS "StageName",

        -- Parse CloseDate (Zieldatum) - Handles DD.MM.YYYY and YYYY-MM-DD
        CASE 
            WHEN op.zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(op.zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
            WHEN op.zieldatum ~ '^\d{4}-\d{2}-\d{2}$' THEN op.zieldatum
            ELSE NULL
        END AS "CloseDate",

        -- Parse Amount - Handle European formatting (e.g. 1.234,56) and standard formats
        CASE 
            WHEN TRIM(COALESCE(op.auftragswert, '')) = '' THEN NULL
            WHEN op.auftragswert ~ '^\-?\d{1,3}(\.\d{3})+,\d+$' 
                THEN REGEXP_REPLACE(REGEXP_REPLACE(TRIM(op.auftragswert), '[.]', ''), ',', '.')::DOUBLE PRECISION
            WHEN op.auftragswert ~ '^\-?\d+,\d+$' 
                THEN REGEXP_REPLACE(TRIM(op.auftragswert), ',', '.')::DOUBLE PRECISION
            WHEN op.auftragswert ~ '^\-?\d+(\.\d+)?$'
                THEN CAST(TRIM(op.auftragswert) AS DOUBLE PRECISION)
            ELSE NULL
        END AS "Amount",

        -- Currency
        TRIM(op.waehrungscode) AS "CurrencyIsoCode",

        -- AccountId (Join with Accounts to get SFDC-style ID '001' prefix)
        '001' || LPAD(CAST(TRIM(ak.kundennummer) AS VARCHAR), 8, '0') AS "AccountId",

        -- Legacy Keys
        op.opp_kennung AS "Legacy_Opportunity_ID__c",

        -- System fields
        CURRENT_DATE::TEXT AS "CreatedDate",
        CURRENT_DATE::TEXT AS "LastModifiedDate",
        0 AS "IsDeleted"

    from opp_raw op
    left join account_raw ak
        on TRIM(op.kunden_ref) = TRIM(ak.kundennummer)
)
select * from stg_opportunity
where "Id" is not null