{{ config(materialized='table') }}

SELECT
    -- Id: canonical normalized project ID (source natural key)
    CAST(INITCAP(TRIM(s.id)) AS TEXT) AS "Id",
    
    -- Name: human-readable project name
    INITCAP(TRIM(s.name)) AS "Name",
    
    -- Project_Status__c: map multilingual status values to target enum domain
    CASE
        WHEN UPPER(TRIM(s.project_status__c)) IN ('AKTIV', 'ACTIVE', 'LAUFEND') THEN 'Active'
        WHEN UPPER(TRIM(s.project_status__c)) IN ('COMPLETED', 'ABGESCHLOSSEN', 'FERTIG') THEN 'Completed'
        WHEN UPPER(TRIM(s.project_status__c)) IN ('PLANUNG', 'IN PLANUNG') THEN 'In Planning'
        WHEN UPPER(TRIM(s.project_status__c)) IN ('ON HOLD', 'PAUSIERT', 'GEHOLTEN') THEN 'On Hold'
        WHEN UPPER(TRIM(s.project_status__c)) IN ('STORNIERT', 'CANCELLED', 'ABBRECHEN') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    
    -- Go_Live_Date__c: multi-format date parser → ISO YYYY-MM-DD
    CASE
        WHEN s.go_live_date__c ~ '^\d{8}$'
            THEN TO_CHAR(TO_DATE(s.go_live_date__c, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN s.go_live_date__c ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_CHAR(TO_DATE(s.go_live_date__c, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN s.go_live_date__c ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            THEN TO_CHAR(TO_DATE(s.go_live_date__c, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        WHEN s.go_live_date__c ~ '^\d{1,2}\.\d{1,2}\.\d{4}$'
            THEN TO_CHAR(TO_DATE(s.go_live_date__c, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    
    -- Account__c: normalized FK to target Account.Id (strip prefix/suffix for consistency)
    CAST(INITCAP(TRIM(s.account__c)) AS TEXT) AS "Account__c",
    
    -- Opportunity__c: normalized FK to target Opportunity.Id
    CAST(INITCAP(TRIM(s.opportunity__c)) AS TEXT) AS "Opportunity__c",
    
    -- Legacy_Project_ID__c: raw source PK for row-level verification
    INITCAP(TRIM(s.id)) AS "Legacy_Project_ID__c",
    
    -- Audit fields (not in source): deterministic defaults
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'project__c') }} s