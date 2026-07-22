{{ config(materialized='table') }}

-- Helper: parse date formats to ISO text
with src as (
    select * from {{ source('fixture_master_v2_src', 'master_projekte') }}
),
parsed_dates as (
    select *,
        case
            when go_live_datum is null or trim(go_live_datum) = '' then NULL
            -- Try DD.MM.YYYY format
            when go_live_datum ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' then to_char(to_date(trim(go_live_datum), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            -- Try YYYYMMDD format
            when go_live_datum ~ '^\d{8}$' then to_char(to_date(trim(go_live_datum), 'YYYYMMDD'), 'YYYY-MM-DD')
            -- Try MM/DD/YYYY format
            when go_live_datum ~ '^\d{1,2}/\d{1,2}/\d{4}$' then to_char(to_date(trim(go_live_datum), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            else NULL
        end as parsed_go_live_datum
    from src
)

select
    -- Salesforce-style ID derived from source natural key
    SUBSTRING(MD5(projekt_kennung), 1, 18) AS "Id",
    -- Name
    INITCAP(TRIM(projektname)) AS "Name",
    -- Project Status: map to allowed enum values
    CASE UPPER(TRIM(projektstatus))
        WHEN 'ACTIVE' THEN 'Active'
        WHEN 'COMPLETED' THEN 'Completed'
        WHEN 'IN PLANNING' THEN 'In Planning'
        WHEN 'ON HOLD' THEN 'On Hold'
        WHEN 'CANCELLED' THEN 'Cancelled'
        WHEN 'AKTIV' THEN 'Active'
        WHEN 'ABGESCHLOSSEN' THEN 'Completed'
        WHEN 'GEPLANT' THEN 'In Planning'
        WHEN 'PAUSIERT' THEN 'On Hold'
        WHEN 'STORNIERT' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    -- Go Live Date: parsed from source (nullable)
    parsed_go_live_datum AS "Go_Live_Date__c",
    -- Account__c: transform kunden_kennung to same SFDC-style ID as Account.Id
    SUBSTRING(MD5(kunden_kennung), 1, 18) AS "Account__c",
    -- Opportunity__c: transform opp_kennung_ref to same SFDC-style ID as Opportunity.Id
    SUBSTRING(MD5(opp_kennung_ref), 1, 18) AS "Opportunity__c",
    -- Legacy Project ID
    projekt_kennung AS "Legacy_Project_ID__c",
    -- CreatedDate
    CURRENT_DATE::text AS "CreatedDate",
    -- LastModifiedDate
    CURRENT_DATE::text AS "LastModifiedDate",
    -- IsDeleted
    0 AS "IsDeleted"
from parsed_dates
