{{ config(materialized='table') }}

select
    o.id as "Id",
    coalesce(initcap(trim(o.name)), 'Unnamed Opportunity') as "Name",
    case
        when lower(trim(o.stagename)) in ('prospecting', 'prospect', 'prospects', 'in kontakt') then 'Prospecting'
        when lower(trim(o.stagename)) in ('qualification', 'qualifikation', 'quali') then 'Qualification'
        when lower(trim(o.stagename)) in ('in prüfung', 'prüfung', 'needs analysis') then 'Needs Analysis'
        when lower(trim(o.stagename)) in ('value proposition', 'wertversprechen') then 'Value Proposition'
        when lower(trim(o.stagename)) in ('id. decision makers', 'id. entscheidungsträger') then 'Id. Decision Makers'
        when lower(trim(o.stagename)) in ('perception analysis', 'wahrnehmungsanalyse') then 'Perception Analysis'
        when lower(trim(o.stagename)) in ('proposal/price quote', 'angebot/preisanfrage') then 'Proposal/Price Quote'
        when lower(trim(o.stagename)) in ('negotiation/review', 'verhandlung/prüfung') then 'Negotiation/Review'
        when lower(trim(o.stagename)) in ('won', 'gewonnen', 'closed won', 'abgeschlossen (gewonnen)', 'abgeschlossen(gewonnen)') then 'Closed Won'
        when lower(trim(o.stagename)) in ('lost', 'verloren', 'closed lost', 'abgeschlossen (verloren)', 'abgeschlossen(verloren)') then 'Closed Lost'
        else 'Prospecting'
    end as "StageName",
    case
        when o.closedate is null or trim(o.closedate) = '' then null
        when trim(o.closedate) ~ '^\d{8}$' then to_date(trim(o.closedate), 'YYYYMMDD')::text
        when trim(o.closedate) ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' then to_date(trim(o.closedate), 'DD.MM.YYYY')::text
        when trim(o.closedate) ~ '^\d{1,2}/\d{1,2}/\d{4}$' then to_date(trim(o.closedate), 'MM/DD/YYYY')::text
        when trim(o.closedate) ~ '^\d{4}-\d{2}-\d{2}$' then to_date(trim(o.closedate), 'YYYY-MM-DD')::text
        else null
    end as "CloseDate",
    case
        when o.amount is null or trim(o.amount) = '' or lower(trim(o.amount)) = 'none' then null
        else
            cast(
                case
                    -- European format: dot-thousands-comma-decimal (e.g., 101.743,05)
                    when regexp_replace(trim(o.amount), '[^0-9.,\-]', '') ~ '\d+\.\d{3},\d' then
                        regexp_replace(
                            regexp_replace(regexp_replace(trim(o.amount), '[^0-9.,\-]', ''), '\.', ''),
                            ',', '.'
                        )
                    -- Comma thousands standard format (e.g., 1,234,567.89)
                    when regexp_replace(trim(o.amount), '[^0-9.,\-]', '') ~ ',\d{3}' then
                        regexp_replace(regexp_replace(trim(o.amount), '[^0-9.,\-]', ''), ',', '')
                    -- Standard decimal or simple number (e.g., 42543.61 or -120228.71)
                    else regexp_replace(trim(o.amount), '[^0-9.\-]', '')
                end
            as double precision)
    end as "Amount",
    case
        when lower(trim(o.currencyisocode)) in ('usd', 'dollar', '$') then 'USD'
        when lower(trim(o.currencyisocode)) in ('eur', 'euro', '€') then 'EUR'
        when lower(trim(o.currencyisocode)) in ('gbp', 'pound', '£') then 'GBP'
        when lower(trim(o.currencyisocode)) in ('chf', 'swiss franc') then 'CHF'
        else upper(trim(coalesce(o.currencyisocode, '')))
    end as "CurrencyIsoCode",
    a.id as "AccountId",
    o.id as "Legacy_Opportunity_ID__c",
    null::text as "CreatedDate",
    null::text as "LastModifiedDate",
    0 as "IsDeleted"
from {{ source('fixture_messy_data_v2_src', 'opportunity') }} o
left join {{ source('fixture_messy_data_v2_src', 'account') }} a
    on o.accountid = a.id