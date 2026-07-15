{{ config(materialized='table') }}

with opp_raw as (
    select
        mopp.opp_kennung,
        mopp.titel,
        mopp.vertriebsphase,
        mopp.zieldatum,
        mopp.auftragswert,
        mopp.waehrungscode,
        mkun.kundennummer
    from {{ source('fixture_master_v2_src', 'master_opportunities') }} mopp
    left join {{ source('fixture_master_v2_src', 'master_kunden') }} mkun
        on mkun.kundennummer = regexp_replace(mopp.kunden_ref, '^KD-', 'CUST-')
)

select
    -- Id: transform OPP-XXXXX to 006XXXXXXX Salesforce-style ID
    '006' || lpad(regexp_replace(opp_kennung, '^OPP-', ''), 8, '0') as "Id",

    -- Name: opportunity title (NOT NULL — default to unknown if empty)
    coalesce(nullif(trim(titel), ''), 'Unknown Opportunity') as "Name",

    -- StageName: normalize various German/English variants to standard stages
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
        else 'Prospecting'
    end as "StageName",

    -- CloseDate: parse multiple date formats to ISO YYYY-MM-DD
    case
        when zieldatum is null or trim(zieldatum) = '' then NULL
        when trim(zieldatum) ~ '^\d{4}-\d{2}-\d{2}$' then trim(zieldatum)
        when trim(zieldatum) ~ '^\d{8}$' then
            TO_CHAR(TO_DATE(trim(zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
        when trim(zieldatum) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' then
            TO_CHAR(TO_DATE(trim(zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        when trim(zieldatum) ~ '^\d{1,2}/\d{1,2}/\d{4}$' then
            TO_CHAR(TO_DATE(trim(zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        else NULL
    end as "CloseDate",

    -- Amount: clean European notation (strip currency text/symbols, handle dot+comma patterns)
    case
        when auftragswert is null or trim(auftragswert) = '' or upper(trim(auftragswert)) = 'NONE' then NULL
        when trim(auftragswert) ~ '^\s*-$' then NULL
        else
            CAST(
                case
                    -- European format: contains comma → remove dots (thousand sep), swap comma to period, remove currency prefix
                    when regexp_replace(trim(auftragswert), '[^0-9.,€$£\-\s]', '', 'g') ~ ',' then
                        regexp_replace(
                            regexp_replace(regexp_replace(trim(auftragswert), '[^\d.,-]', '', 'g'), '\.', '', 'g'),
                            ',', '.'
                        )
                    -- Standard format: remove non-numeric except minus, periods are decimals
                    else regexp_replace(trim(auftragswert), '[^\d.,-]', '', 'g')
                end
            AS DOUBLE PRECISION)
    end as "Amount",

    -- CurrencyIsoCode: normalize currency representations to ISO codes
    case lower(trim(waehrungscode))
        when 'usd' then 'USD'
        when 'eur' then 'EUR'
        when 'gbp' then 'GBP'
        when 'chf' then 'CHF'
        when '$' then 'USD'
        when 'euro' then 'EUR'
        when '€' then 'EUR'
        when '£' then 'GBP'
        when 'dollar' then 'USD'
        else upper(trim(waehrungscode))
    end as "CurrencyIsoCode",

    -- AccountId: reference to Salesforce Account Id (CUST-MXXXX → 001XXXXXXX)
    case
        when kundennummer is not null and kundennummer ~ '^CUST-M\d+$' then
            '001' || lpad(regexp_replace(kundennummer, '^CUST-', ''), 8, '0')
        else NULL
    end as "AccountId",

    -- Legacy_Opportunity_ID__c: the original opp_kennung natural key
    opp_kennung as "Legacy_Opportunity_ID__c",

    -- Audit fields with defaults
    CAST(now() AS TEXT) as "CreatedDate",
    CAST(now() AS TEXT) as "LastModifiedDate",
    0 as "IsDeleted"

from opp_raw