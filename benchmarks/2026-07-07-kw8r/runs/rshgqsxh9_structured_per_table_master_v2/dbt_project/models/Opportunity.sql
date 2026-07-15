{{ config(materialized='table') }}

with opp_raw as (
    select
        mopp.opp_kennung,
        mopp.titel,
        mopp.vertriebsphase,
        mopp.zieldatum,
        mopp.auftragswert,
        mopp.waehrungscode,
        mopp.kunden_ref,
        mkun.kundennummer as account_id_raw
    from {{ source('fixture_master_v2_src', 'master_opportunities') }} mopp
    left join {{ source('fixture_master_v2_src', 'master_kunden') }} mkun
        on mkun.kundennummer = mopp.kunden_ref
)

select
    -- Id: Salesforce-style ID from opp_kennung with OPP- prefix removed and transformed to CUST format for cross-reference, but since this is Opportunity we use OPP-XXXXX -> 006XXXXXXX pattern would be ideal, but source uses OPP-XXXXX. We'll keep the natural key as legacy and generate a simple ID
    '006' || lpad(regexp_replace(opp_kennung, '^OPP-', ''), 8, '0') as "Id",
    
    -- Name: title of the opportunity
    titel as "Name",
    
    -- StageName: mapped from various German/English variants to standard Salesforce stages
    case lower(trim(vertriebsphase))
        when 'in kontakt' then 'Prospecting'
        when 'in prüfung' then 'Qualification'
        when 'prospecting' then 'Prospecting'
        when 'prospects' then 'Prospecting'
        when 'prospect' then 'Prospecting'
        when 'qualification' then 'Qualification'
        when 'quali' then 'Qualification'
        when 'qualifikation' then 'Qualification'
        when 'needs analysis' then 'Needs Analysis'
        when 'value proposition' then 'Value Proposition'
        when 'id. decision makers' then 'Id. Decision Makers'
        when 'perception analysis' then 'Perception Analysis'
        when 'proposal/price quote' then 'Proposal/Price Quote'
        when 'negotiation/review' then 'Negotiation/Review'
        when 'abgeschlossen (gewonnen)' then 'Closed Won'
        when 'closed won' then 'Closed Won'
        when 'won' then 'Closed Won'
        when 'gewonnen' then 'Closed Won'
        when 'abgeschlossen (verloren)' then 'Closed Lost'
        when 'closed lost' then 'Closed Lost'
        when 'lost' then 'Closed Lost'
        when 'verloren' then 'Closed Lost'
        else NULL
    end as "StageName",
    
    -- CloseDate: parse multiple date formats to ISO YYYY-MM-DD
    case 
        when zieldatum is null or trim(zieldatum) = '' then NULL
        when zieldatum ~ '^\d{4}-\d{2}-\d{2}$' then zieldatum  -- ISO format already
        when zieldatum ~ '^\d{8}$' then 
            TO_CHAR(TO_DATE(zieldatum, 'YYYYMMDD'), 'YYYY-MM-DD')
        when zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' then
            TO_CHAR(TO_DATE(zieldatum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        when zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' then
            TO_CHAR(TO_DATE(zieldatum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        else NULL
    end as "CloseDate",
    
    -- Amount: clean European notation (remove EUR prefix, handle dots as thousand separators and comma as decimal)
    case 
        when auftragswert is null or trim(auftragswert) = '' or upper(trim(auftragswert)) = 'NONE' then NULL
        else
            cast(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(auftragswert, '[^\d.,-]', '', 'g'),  -- Remove currency symbols and non-numeric chars
                        '\.', '', 'g'  -- Remove thousand separator dots
                    ),
                    ',', '.'  -- Swap comma decimal to period
                )::DOUBLE PRECISION
            )
    end as "Amount",
    
    -- CurrencyIsoCode: normalize various currency representations to ISO codes
    case lower(trim(waehrungscode))
        when 'usd' then 'USD'
        when 'eur' then 'EUR'
        when 'gbp' then 'GBP'
        when 'chf' then 'CHF'
        when '$' then 'USD'
        when 'dollar' then 'USD'
        when 'euro' then 'EUR'
        when '€' then 'EUR'
        when '£' then 'GBP'
        else upper(trim(waehrungscode))
    end as "CurrencyIsoCode",
    
    -- AccountId: reference to Account.Id using the KD-MXXXX -> CUST-MXXXX transformation
    case 
        when account_id_raw is not null and account_id_raw ~ '^KD-M\d+$' then '001' || lpad(regexp_replace(account_id_raw, '^KD-', ''), 8, '0')
        else NULL
    end as "AccountId",
    
    -- Legacy_Opportunity_ID__c: the original opp_kennung
    opp_kennung as "Legacy_Opportunity_ID__c",
    
    -- Audit fields with defaults
    cast(now() as text) as "CreatedDate",
    cast(now() as text) as "LastModifiedDate",
    0 as "IsDeleted"

from opp_raw