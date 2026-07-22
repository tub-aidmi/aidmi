{{ config(materialized='table') }}

SELECT
    MD5(mk.kontakt_id) AS "Id",
    INITCAP(TRIM(mk.rufname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(mk.familienname)), 'Unknown Last Name') AS "LastName",
    LOWER(TRIM(mk.kontakt_email)) AS "Email",
    TRIM(mk.tel) AS "Phone",
    INITCAP(TRIM(mk.berufsbezeichnung)) AS "Title",
    CASE
        WHEN LOWER(TRIM(mk.rolle)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(mk.rolle)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(mk.rolle)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(mk.rolle)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(mk.korrespondenzsprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(mkd.kundennummer) AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk
LEFT JOIN
    {{ source('fixture_master_v2_src', 'master_kunden') }} AS mkd
    ON mk.kd_nummer = mkd.kundennummer
```