{{ config(materialized='table') }}

WITH contacts AS (
    SELECT
        -- Id: use kontakt_id as primary key (Salesforce-compatible)
        k.kontakt_id AS "Id",

        -- FirstName
        TRIM(k.rufname) AS "FirstName",

        -- LastName NOT NULL - use default if missing
        COALESCE(TRIM(k.familienname), 'Unknown') AS "LastName",

        -- Email
        TRIM(k.kontakt_email) AS "Email",

        -- Phone: normalize to digits, keep leading + if present
        CASE
            WHEN k.tel IS NULL OR TRIM(k.tel) = '' THEN NULL
            ELSE REGEXP_REPLACE(k.tel, '[^0-9+]', '', 'g')
        END AS "Phone",

        -- Title
        INITCAP(TRIM(k.berufsbezeichnung)) AS "Title",

        -- Role__c: map to allowed enum values
        CASE
            WHEN TRIM(LOWER(COALESCE(k.rolle, ''))) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
            WHEN TRIM(LOWER(COALESCE(k.rolle, ''))) = 'end user' THEN 'End User'
            WHEN TRIM(LOWER(COALESCE(k.rolle, ''))) = 'technical contact' THEN 'Technical Contact'
            WHEN TRIM(LOWER(COALESCE(k.rolle, ''))) IN ('executive sponsor', 'geschäftsführer') THEN 'Executive Sponsor'
            ELSE NULL
        END AS "Role__c",

        -- Preferred_Language__c: normalize to allowed codes
        CASE
            WHEN TRIM(LOWER(COALESCE(k.korrespondenzsprache, ''))) IN ('de', 'deutsch') THEN 'DE'
            WHEN TRIM(LOWER(COALESCE(k.korrespondenzsprache, ''))) IN ('en', 'englisch', 'english') THEN 'EN'
            WHEN TRIM(LOWER(COALESCE(k.korrespondenzsprache, ''))) = 'fr' THEN 'FR'
            WHEN TRIM(LOWER(COALESCE(k.korrespondenzsprache, ''))) = 'es' OR TRIM(LOWER(COALESCE(k.korrespondenzsprache, ''))) IN ('spanisch', 'spanish') THEN 'ES'
            WHEN TRIM(LOWER(COALESCE(k.korrespondenzsprache, ''))) = 'it' OR TRIM(LOWER(COALESCE(k.korrespondenzsprache, ''))) IN ('italienisch', 'italian') THEN 'IT'
            ELSE NULL
        END AS "Preferred_Language__c",

        -- AccountId: foreign key to Account (via master_kunden.kundennummer)
        TRIM(k.kd_nummer) AS "AccountId",

        -- Legacy_Contact_ID__c: same as Id for lineage tracking
        k.kontakt_id AS "Legacy_Contact_ID__c",

        -- CreatedDate / LastModifiedDate: not present in source, default NULL
        NULL::TEXT AS "CreatedDate",
        NULL::TEXT AS "LastModifiedDate",

        -- IsDeleted: no deletion indicator in source
        0::INTEGER AS "IsDeleted"

    FROM {{ source('fixture_master_src', 'master_kontakte') }} k
)

SELECT * FROM contacts