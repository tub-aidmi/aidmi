{{ config(materialized='table') }}

with source_data as (
    select
        m_opp.opp_kennung,
        m_opp.titel,
        m_opp.vertriebsphase,
        m_opp.zieldatum,
        m_opp.auftragswert,
        m_opp.waehrungscode,
        trim(m_opp.kunden_ref) as raw_acc_ref_key
    from {{ source('fixture_master_v2_src', 'master_opportunities') }} m_opp
    left join {{ source('fixture_master_v2_src', 'master_kunden') }} m_acc
        on trim(m_opp.kunden_ref) = trim(m_acc.kundennummer)
),

cleaned as (
    select
        *,
        regexp_replace(trim(auftragswert), '[^0-9.,]', '', 'g') as clean_amt_str
    from source_data
)

select
    'O0XX' || coalesce(opp_kennung, '') as "Id",
    coalesce(trim(titel), 'Unnamed Opportunity') as "Name",
    case lower(trim(vertriebsphase))
        when 'inquiry' then 'Prospecting'
        when 'neu' then 'Prospecting'
        when 'neue chancen' then 'Prospecting'
        when 'prospecting' then 'Prospecting'
        when 'qualifizierung' then 'Qualification'
        when 'qualification' then 'Qualification'
        when 'verifizierung' then 'Qualification'
        when 'bedarfsanalyse' then 'Needs Analysis'
        when 'needs analysis' then 'Needs Analysis'
        when 'discovery' then 'Needs Analysis'
        when 'lösungsdefinition' then 'Value Proposition'
        when 'value proposition' then 'Value Proposition'
        when 'konzept' then 'Value Proposition'
        when 'decision makers identified' then 'Id. Decision Makers'
        when 'key person identified' then 'Id. Decision Makers'
        when 'entscheidungsträger identifiziert' then 'Id. Decision Makers'
        when 'wahrnehmungsanalyse' then 'Perception Analysis'
        when 'perception analysis' then 'Perception Analysis'
        when 'bewertung' then 'Perception Analysis'
        when 'angebot' then 'Proposal/Price Quote'
        when 'proposal' then 'Proposal/Price Quote'
        when 'quote' then 'Proposal/Price Quote'
        when 'preisangebot' then 'Proposal/Price Quote'
        when 'verhandlung' then 'Negotiation/Review'
        when 'negotiation' then 'Negotiation/Review'
        when 'checkpoint' then 'Negotiation/Review'
        when 'gewonnen' then 'Closed Won'
        when 'won' then 'Closed Won'
        when 'abschluss' then 'Closed Won'
        when 'verloren' then 'Closed Lost'
        when 'lost' then 'Closed Lost'
        when 'abgelehnt' then 'Closed Lost'
        else 'Prospecting'
    end as "StageName",
    case
        when trim(zieldatum) is null or trim(zieldatum) = '' then cast(null as text)
        when zieldatum ~ '^\d{4}-\d{2}-\d{2}$' then trim(zieldatum)
        when zieldatum ~ '^\d{2}\.\d{2}\.\d{4}$' then to_date(trim(zieldatum), 'DD.MM.YYYY')::text
        when length(trim(zieldatum)) = 8 and zieldatum ~ '^\d{8}$' then substr(trim(zieldatum), 1, 4) || '-' || substr(trim(zieldatum), 5, 2) || '-' || substr(trim(zieldatum), 7, 2)
        else cast(null as text)
    end as "CloseDate",
    case 
        when clean_amt_str is null or trim(clean_amt_str) = '' then cast(null as double precision)
        when clean_amt_str ~ '\.' and clean_amt_str ~ ',' then
            cast(regexp_replace(regexp_replace(clean_amt_str, '\.', '', 'g'), ',', '.', 'g') as double precision)
        when clean_amt_str ~ ',' then
            cast(replace(clean_amt_str, ',', '.') as double precision)
        else
            cast(clean_amt_str as double precision)
    end as "Amount",
    coalesce(trim(waehrungscode), 'EUR') as "CurrencyIsoCode",
    case when trim(raw_acc_ref_key) is not null and trim(raw_acc_ref_key) != '' then 'A0XX' || trim(raw_acc_ref_key) else cast(null as text) end as "AccountId",
    coalesce(opp_kennung, '') as "Legacy_Opportunity_ID__c",
    current_timestamp::text as "CreatedDate",
    current_timestamp::text as "LastModifiedDate",
    0 as "IsDeleted"

from cleaned