{{ config(materialized='table') }}

-- Helper: parse European date formats (DD.MM.YYYY, YYYYMMDD, MM/DD/YYYY) to ISO text
with src as (
    select * from {{ source('fixture_master_v2_src', 'master_opportunities') }}
),
parsed_dates as (
    select *,
        case
            when zieldatum is null or trim(zieldatum) = '' then NULL
            -- Try DD.MM.YYYY format
            when zieldatum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' then to_char(to_date(trim(zieldatum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            -- Try YYYYMMDD format
            when zieldatum ~ '^\d{8}$' then to_char(to_date(trim(zieldatum), 'YYYYMMDD'), 'YYYY-MM-DD')
            -- Try MM/DD/YYYY format
            when zieldatum ~ '^\d{1,2}/\d{1,2}/\d{4}$' then to_char(to_date(trim(zieldatum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            else NULL
        end as parsed_zieldatum
    from src
)

select
    -- Salesforce-style ID derived from source natural key
    SUBSTRING(MD5(opp_kennung), 1, 18) AS "Id",
    -- Name
    INITCAP(TRIM(titel)) AS "Name",
    -- StageName: map vertriebsphase to allowed enum values
    CASE UPPER(TRIM(vertriebsphase))
        WHEN 'PROSPECTING' THEN 'Prospecting'
        WHEN 'QUALIFICATION' THEN 'Qualification'
        WHEN 'NEEDS ANALYSIS' THEN 'Needs Analysis'
        WHEN 'VALUE PROPOSITION' THEN 'Value Proposition'
        WHEN 'ID. DECISION MAKERS' THEN 'Id. Decision Makers'
        WHEN 'PERCEPTION ANALYSIS' THEN 'Perception Analysis'
        WHEN 'PROPOSAL/PRICE QUOTE' THEN 'Proposal/Price Quote'
        WHEN 'NEGOTIATION/REVIEW' THEN 'Negotiation/Review'
        WHEN 'CLOSED WON' THEN 'Closed Won'
        WHEN 'CLOSED LOST' THEN 'Closed Lost'
        WHEN 'VORQUALIFIZIERUNG' THEN 'Prospecting'
        WHEN 'QUALIFIZIERUNG' THEN 'Qualification'
        WHEN 'BEDARFSANALYSE' THEN 'Needs Analysis'
        WHEN 'WERTPROPOSITION' THEN 'Value Proposition'
        WHEN 'ENTSCHEIDERIDENTIFIKATION' THEN 'Id. Decision Makers'
        WHEN 'WAHRNEHMUNGSANALYSE' THEN 'Perception Analysis'
        WHEN 'ANGEBOT/PREISANFRAGE' THEN 'Proposal/Price Quote'
        WHEN 'VERHANDLUNG/ÜBERPRÜFUNG' THEN 'Negotiation/Review'
        WHEN 'GEWONNEN GESCHLOSSEN' THEN 'Closed Won'
        WHEN 'VERLOREN GESCHLOSSEN' THEN 'Closed Lost'
        ELSE NULL
    END AS "StageName",
    -- CloseDate: parsed date from zieldatum (NOT NULL in target, so use default if unparseable)
    coalesce(parsed_zieldatum, CURRENT_DATE::text) AS "CloseDate",
    -- Amount: strip currency symbols and handle European number formats
    case
        when auftragswert is null or trim(auftragswert) = '' then NULL
        else cast(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        trim(auftragswert), '[^\d.,]', '', 'g'  -- remove currency symbols/text
                    ),
                    '\.', ''  -- remove thousand separator dots (European format)
                ),
                ',', '.' -- convert decimal comma to dot
            )::double precision
        end
    end AS "Amount",
    -- Currency ISO Code
    waehrungscode AS "CurrencyIsoCode",
    -- AccountId: transform kunden_ref to same SFDC-style ID as Account.Id
    SUBSTRING(MD5(kunden_ref), 1, 18) AS "AccountId",
    -- Legacy Opportunity ID
    opp_kennung AS "Legacy_Opportunity_ID__c",
    -- CreatedDate
    CURRENT_DATE::text AS "CreatedDate",
    -- LastModifiedDate
    CURRENT_DATE::text AS "LastModifiedDate",
    -- IsDeleted
    0 AS "IsDeleted"
from parsed_dates
