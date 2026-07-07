SELECT
    MD5(p.projekt_kennung) AS "Id",
    COALESCE(p.projektname, 'Unknown Project') AS "Name",
    CASE
        WHEN LOWER(p.projektstatus) IN ('active', 'aktiv') THEN 'Active'
        WHEN LOWER(p.projektstatus) IN ('completed', 'abgeschlossen') THEN 'Completed'
        WHEN LOWER(p.projektstatus) IN ('in planning', 'in planung') THEN 'In Planning'
        WHEN LOWER(p.projektstatus) IN ('on hold', 'angehalten') THEN 'On Hold'
        WHEN LOWER(p.projektstatus) IN ('cancelled', 'storniert') THEN 'Cancelled'
        ELSE NULL
    END AS "Project_Status__c",
    CASE
        WHEN p.go_live_datum ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}\.\d{2}\.\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'DD.MM.YYYY'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{8}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'YYYYMMDD'), 'YYYY-MM-DD')
        WHEN p.go_live_datum ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_CHAR(TO_DATE(p.go_live_datum, 'MM/DD/YYYY'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Go_Live_Date__c",
    MD5(k.kundennummer) AS "Account__c",
    MD5(o.opp_kennung) AS "Opportunity__c",
    p.projekt_kennung AS "Legacy_Project_ID__c",
    '2023-01-01T00:00:00Z' AS "CreatedDate",
    '2023-01-01T00:00:00Z' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_projekte') }} AS p
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS k
ON
    p.kunden_kennung = k.kundennummer
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_opportunities') }} AS o
ON
    p.opp_kennung_ref = o.opp_kennung