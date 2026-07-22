{{ config(materialized='table') }}

SELECT
    'con_' || mk.kontakt_id AS "Id",
    TRIM(mk.rufname) AS "FirstName",
    TRIM(mk.familienname) AS "LastName",
    LOWER(TRIM(mk.kontakt_email)) AS "Email",
    TRIM(mk.tel) AS "Phone",
    TRIM(mk.berufsbezeichnung) AS "Title",
    CASE
        WHEN UPPER(TRIM(mk.rolle)) LIKE '%ENTSCHEIDER%' THEN 'Decision Maker'
        WHEN UPPER(TRIM(mk.rolle)) LIKE '%ENDNUTZER%' THEN 'End User'
        WHEN UPPER(TRIM(mk.rolle)) LIKE '%TECHNIK%' OR UPPER(TRIM(mk.rolle)) LIKE '%TECHNISCH%' THEN 'Technical Contact'
        WHEN UPPER(TRIM(mk.rolle)) LIKE '%GESCHÄFTSFÜHRER%' OR UPPER(TRIM(mk.rolle)) LIKE '%EXEKUTIV%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) LIKE '%DEUTSCH%' THEN 'DE'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) LIKE '%ENGLISCH%' THEN 'EN'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) LIKE '%FRANZÖSISCH%' OR UPPER(TRIM(mk.korrespondenzsprache)) LIKE '%FRANCAIS%' THEN 'FR'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) LIKE '%SPANISCH%' THEN 'ES'
        WHEN UPPER(TRIM(mk.korrespondenzsprache)) LIKE '%ITALIENISCH%' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    'acc_' || mkd.kundennummer AS "AccountId",
    mk.kontakt_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} mk
LEFT JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mkd
    ON TRIM(mk.kd_nummer) = TRIM(mkd.kundennummer)