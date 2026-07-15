{{ config(materialized='table') }}

WITH accounts AS (
    SELECT 
        LOWER(
            CONCAT(
                CASE 
                    WHEN kunden_nr LIKE 'K%' THEN '001'
                    ELSE '033'
                END,
                LPAD((MD5(kunden_nr)::bit(20)::bigint::text), 15, '0')
            )
        ) AS "Id",
        LOWER(REGEXP_REPLACE(kunden_nr, '[^A-Z0-9]+', '', 'g')) AS normalized_customer_key
    FROM {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}
),
projects AS (
    SELECT 
        LOWER(
            CONCAT(
                CASE 
                    WHEN proj_id LIKE 'P%' THEN '006'
                    ELSE '033'
                END,
                LPAD((MD5(proj_id)::bit(20)::bigint::text), 15, '0')
            )
        ) AS "Id",
        LOWER(REGEXP_REPLACE(proj_id, '[^A-Z0-9]+', '', 'g')) AS normalized_project_key
    FROM {{ source('fixture_wrong_field_names_v2_src', 'proj') }}
)

SELECT
    -- Id: deterministic Salesforce-style 18-char ID from normalized asset_id
    LOWER(
        CONCAT(
            CASE 
                WHEN asset_id LIKE 'A%' THEN '001'
                WHEN asset_id LIKE 'C%' THEN '003'
                WHEN asset_id LIKE 'K%' THEN '005'
                WHEN asset_id LIKE 'P%' THEN '006'
                WHEN asset_id LIKE 'CH%' THEN '008'
                ELSE '033'
            END,
            LPAD((MD5(asset_id)::bit(20)::bigint::text), 15, '0')
        )
    ) AS "Id",

    -- Name: bezeichnung with INITCAP and TRIM
    INITCAP(TRIM(bezeichnung)) AS "Name",

    -- Serial_Number__c
    TRIM(seriennr) AS "Serial_Number__c",

    -- Warranty_End_Date__c: layered date parsing for DD.MM.YYYY, YYYYMMDD, MM/DD/YYYY formats
    CASE 
        WHEN garantie_bis IS NULL OR TRIM(garantie_bis) = '' THEN NULL
        WHEN garantie_bis ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(TRIM(garantie_bis), 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN garantie_bis ~ '^\d{8}$' THEN 
            SUBSTR(TRIM(garantie_bis), 1, 4) || '-' || SUBSTR(TRIM(garantie_bis), 5, 2) || '-' || SUBSTR(TRIM(garantie_bis), 7, 2)
        WHEN garantie_bis ~ '^\d{2}/\d{2}/\d{4}$' THEN 
            TO_CHAR(TO_DATE(TRIM(garantie_bis), 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",

    -- Account__c: deterministic Salesforce-style Account Id from joined kunden
    accounts."Id" AS "Account__c",

    -- Project__c: deterministic Salesforce-style Project__c Id from joined proj
    projects."Id" AS "Project__c",

    -- Legacy_Asset_ID__c: original asset_id trimmed
    TRIM(asset_id) AS "Legacy_Asset_ID__c",

    -- System columns
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_wrong_field_names_v2_src', 'assets') }} assets

-- Join to Account via normalized kd_ref key → accounts.normalized_customer_key
LEFT JOIN accounts 
    ON accounts.normalized_customer_key = UPPER(REGEXP_REPLACE(TRIM(assets.kd_ref), '[^A-Z0-9]+', '', 'g'))

-- Join to Project__c via normalized projekt_ref key → projects.normalized_project_key
LEFT JOIN projects 
    ON projects.normalized_project_key = UPPER(REGEXP_REPLACE(TRIM(assets.projekt_ref), '[^A-Z0-9]+', '', 'g'))