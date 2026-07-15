{{ config(materialized='table') }}

-- Helper: parse date formats to ISO text
with src as (
    select * from {{ source('fixture_master_v2_src', 'master_assets') }}
),
parsed_dates as (
    select *,
        case
            when garantieende is null or trim(garantieende) = '' then NULL
            -- Try DD.MM.YYYY format
            when garantieende ~ '^\d{1,2}\.\d{1,2}\.\d{4}$' then to_char(to_date(trim(garantieende), 'DD.MM.YYYY'), 'YYYY-MM-DD')
            -- Try YYYYMMDD format
            when garantieende ~ '^\d{8}$' then to_char(to_date(trim(garantieende), 'YYYYMMDD'), 'YYYY-MM-DD')
            -- Try MM/DD/YYYY format
            when garantieende ~ '^\d{1,2}/\d{1,2}/\d{4}$' then to_char(to_date(trim(garantieende), 'MM/DD/YYYY'), 'YYYY-MM-DD')
            else NULL
        end as parsed_garantieende
    from src
)

select
    -- Salesforce-style ID derived from source natural key
    SUBSTRING(MD5(asset_kennung), 1, 18) AS "Id",
    -- Name
    INITCAP(TRIM(asset_name)) AS "Name",
    -- Serial Number
    serien_nummer AS "Serial_Number__c",
    -- Warranty End Date: parsed from source (nullable)
    parsed_garantieende AS "Warranty_End_Date__c",
    -- Account__c: transform kunden_kennung to same SFDC-style ID as Account.Id
    SUBSTRING(MD5(kunden_kennung), 1, 18) AS "Account__c",
    -- Project__c: transform projekt_kennung to same SFDC-style ID as Project__c.Id
    SUBSTRING(MD5(projekt_kennung), 1, 18) AS "Project__c",
    -- Legacy Asset ID
    asset_kennung AS "Legacy_Asset_ID__c",
    -- CreatedDate
    CURRENT_DATE::text AS "CreatedDate",
    -- LastModifiedDate
    CURRENT_DATE::text AS "LastModifiedDate",
    -- IsDeleted
    0 AS "IsDeleted"
from parsed_dates
