{{ config(materialized='table') }}

SELECT
    -- Salesforce-style Id for custom object Project__c (starts with '00I' prefix)
    '00I' || SUBSTRING(MD5(projekt_kennung), 1, 13) AS "Id",

    -- Project name from source
    projektname AS "Name",

    -- Map German project statuses to English enum values
    CASE LOWER(TRIM(projektstatus))
        WHEN 'aktiv'     THEN 'Active'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planung'  THEN 'In Planning'
        WHEN 'pausiert'    THEN 'On Hold'
        WHEN 'auf eis'     THEN 'On Hold'
        WHEN 'storniert'   THEN 'Cancelled'
        WHEN 'abgebrochen' THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",

    -- Go_Live_Date: parse DD.MM.YYYY or YYYY-MM-DD text to ISO format; NULL if unparseable
    CASE
        WHEN go_live_datum IS NOT NULL AND go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN go_live_datum IS NOT NULL AND go_live_datum ~ '^\d{4}-\d{2}-\d{2}$'
            THEN go_live_datum
        WHEN go_live_datum IS NOT NULL AND go_live_datum ~ '^\d{8}$'
            THEN SUBSTRING(go_live_datum, 1, 4) || '-' ||
                 SUBSTRING(go_live_datum, 5, 2) || '-' ||
                 SUBSTRING(go_live_datum, 7, 2)
        ELSE NULL
    END AS "Go_Live_Date__c",

    -- Account__c: transform source kunden_kennung to match Account.Id pattern ('001' + hash)
    '001' || SUBSTRING(MD5(kunden_kennung), 1, 13) AS "Account__c",

    -- Opportunity__c: transform source opp_kennung_ref to match Opportunity.Id pattern ('006' + hash)
    '006' || SUBSTRING(MD5(opp_kennung_ref), 1, 13) AS "Opportunity__c",

    -- Legacy key for row-level verification
    projekt_kennung AS "Legacy_Project_ID__c",

    -- Standard Salesforce audit fields (defaults for static fixture data)
    '2024-01-01T00:00:00.000+0000' AS "CreatedDate",
    '2024-01-01T00:00:00.000+0000' AS "LastModifiedDate",
    0                                    AS "IsDeleted"

FROM {{ source('fixture_master_v2_src', 'master_projekte') }}