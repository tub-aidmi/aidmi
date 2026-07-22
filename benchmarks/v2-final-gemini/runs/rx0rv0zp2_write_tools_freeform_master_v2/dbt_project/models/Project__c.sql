{{ config(materialized='table') }}

SELECT
    projekt_kennung AS "Id",
    COALESCE(TRIM(projektname), 'N/A') AS "Name",
    CASE LOWER(TRIM(projektstatus))
        WHEN 'active' THEN 'Active'
        WHEN 'aktiv' THEN 'Active'
        WHEN 'completed' THEN 'Completed'
        WHEN 'abgeschlossen' THEN 'Completed'
        WHEN 'in planning' THEN 'In Planning'
        WHEN 'in planung' THEN 'In Planning'
        WHEN 'planung' THEN 'In Planning'
        WHEN 'on hold' THEN 'On Hold'
        WHEN 'pausiert' THEN 'On Hold'
        WHEN 'cancelled' THEN 'Cancelled'
        WHEN 'storniert' THEN 'Cancelled'
        ELSE 'In Planning' -- Default for NOT NULL
    END AS "Project_Status__c",
    COALESCE(
        TO_CHAR(
            CASE
                WHEN go_live_datum = '0000-00-00' THEN NULL
                WHEN go_live_datum ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN TO_DATE(go_live_datum, 'YYYY-MM-DD')
                WHEN go_live_datum ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' THEN TO_DATE(go_live_datum, 'DD.MM.YYYY')
                WHEN go_live_datum ~ '^\\d{1,2}/\\d{1,2}/\\d{4}$' THEN TO_DATE(go_live_datum, 'MM/DD/YYYY')
                WHEN go_live_datum ~ '^\\d{8}$' THEN TO_DATE(go_live_datum, 'YYYYMMDD')
                ELSE NULL
            END,
            'YYYY-MM-DD'
        ),
        TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD')
    ) AS "Go_Live_Date__c",
    kunden_kennung AS "Account__c",
    REPLACE(opp_kennung_ref, 'OPP-M-', 'OPP-') AS "Opportunity__c",
    projekt_kennung AS "Legacy_Project_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source(source_name, source_table) }}
