{{ config(materialized='table') }}

SELECT
    'C' || LPAD(SUBSTRING(kontakt_id FROM '\d+')::INT, 8, '0') AS "Id",
    INITCAP(TRIM(rufname)) AS "FirstName",
    INITCAP(TRIM(familienname)) AS "LastName",
    kontakt_email AS "Email",
    REGEXP_REPLACE(tel, '[^0-9+]', '', 'g') AS "Phone",
    INITCAP(TRIM(berufsbezeichnung)) AS "Title",
    CASE LOWER(TRIM(COALESCE(rolle, '')))
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'technischer ansprechpartner' THEN 'Technical Contact'
        WHEN 'techniker' THEN 'Technical Contact'
        WHEN 'end user' THEN 'End User'
        WHEN 'endanwender' THEN 'End User'
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'entscheider' THEN 'Decision Maker'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        WHEN 'sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(COALESCE(korrespondenzsprache, '')))
        WHEN 'DE' THEN 'DE'
        WHEN 'deutsch' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        WHEN 'FRENCH' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    CASE WHEN kd_nummer ~ '^CUST-M\d+$'
        THEN 'A' || LPAD(SUBSTRING(kd_nummer FROM '\d+')::INT, 8, '0')
        ELSE NULL
    END AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    '2023-01-01' AS "CreatedDate",
    '2023-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
