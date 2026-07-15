{{ config(materialized='table') }}

WITH contacts AS (
    SELECT
        -- Contact Id: Salesforce-style format using kontakt_id with '003' prefix
        '003' || RIGHT('0000000000' || contact_raw.kontakt_id, 10) AS "Id",

        -- Name fields
        TRIM(contact_raw.rufname) AS "FirstName",
        TRIM(contact_raw.familienname) AS "LastName",

        -- Contact details
        LOWER(TRIM(contact_raw.kontakt_email)) AS "Email",
        TRIM(contact_raw.tel) AS "Phone",
        INITCAP(TRIM(contact_raw.berufsbezeichnung)) AS "Title",

        -- Role mapping to target enum: (Decision Maker, End User, Technical Contact, Executive Sponsor)
        CASE LOWER(TRIM(contact_raw.rolle))
            WHEN 'entscheider' THEN 'Decision Maker'
            WHEN 'endverbraucher' THEN 'End User'
            WHEN 'technischer kontakt' THEN 'Technical Contact'
            WHEN 'technical contact' THEN 'Technical Contact'
            WHEN 'executive sponsor' THEN 'Executive Sponsor'
            WHEN 'vorgesetzter' THEN 'Executive Sponsor'
            WHEN 'leitung' THEN 'Executive Sponsor'
            ELSE NULL
        END AS "Role__c",

        -- Preferred Language mapping to target enum: (DE, EN, FR, ES, IT)
        CASE UPPER(TRIM(contact_raw.korrespondenzsprache))
            WHEN 'DE' THEN 'DE'
            WHEN 'GERMAN' THEN 'DE'
            WHEN 'DEU' THEN 'DE'
            WHEN 'DT' THEN 'DE'
            WHEN 'EN' THEN 'EN'
            WHEN 'ENGLISH' THEN 'EN'
            WHEN 'ENG' THEN 'EN'
            WHEN 'FR' THEN 'FR'
            WHEN 'FRENCH' THEN 'FR'
            WHEN 'FRE' THEN 'FR'
            WHEN 'ES' THEN 'ES'
            WHEN 'SPANISH' THEN 'ES'
            WHEN 'ESP' THEN 'ES'
            WHEN 'IT' THEN 'IT'
            WHEN 'ITALIAN' THEN 'IT'
            WHEN 'ITA' THEN 'IT'
            ELSE NULL
        END AS "Preferred_Language__c",

        -- AccountId: Salesforce-style Account Id derived from source customer number via join to master_kunden
        '001' || RIGHT('0000000000' || mk.kundennummer, 10) AS "AccountId",

        -- Legacy contact ID from source natural key
        contact_raw.kontakt_id AS "Legacy_Contact_ID__c",

        -- Date fields: not present in source contacts table; use NULL per guidelines
        NULL AS "CreatedDate",
        NULL AS "LastModifiedDate",

        -- Deleted flag: 0 = active, 1 = deleted (assuming all contacts are active)
        0 AS "IsDeleted"

    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} contact_raw
    INNER JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
        ON TRIM(contact_raw.kd_nummer) = TRIM(mk.kundennummer)
)

SELECT * FROM contacts