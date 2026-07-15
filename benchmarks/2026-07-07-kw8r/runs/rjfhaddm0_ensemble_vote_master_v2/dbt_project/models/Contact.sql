{{ config(materialized='table') }}

WITH contacts AS (
    SELECT
        -- Contact Id: Salesforce-style format using kontakt_id
        -- Using '003' prefix pattern for Contacts in SFDC convention
        '003' || RIGHT('0000000000' || contact_raw.kontakt_id, 10) AS "Id",

        -- Name fields
        TRIM(contact_raw.rufname) AS "FirstName",
        TRIM(contact_raw.familienname) AS "LastName",

        -- Contact details
        LOWER(TRIM(contact_raw.kontakt_email)) AS "Email",
        TRIM(contact_raw.tel) AS "Phone",
        INITCAP(TRIM(contact_raw.berufsbezeichnung)) AS "Title",

        -- Role mapping to target enum: (Decision Maker, End User, Technical Contact, Executive Sponsor)
        CASE
            WHEN LOWER(TRIM(contact_raw.rolle)) = 'entscheider' THEN 'Decision Maker'
            WHEN LOWER(TRIM(contact_raw.rolle)) = 'endverbraucher' THEN 'End User'
            WHEN LOWER(TRIM(contact_raw.rolle)) IN ('technischer kontakt', 'technical contact') THEN 'Technical Contact'
            WHEN LOWER(TRIM(contact_raw.rolle)) IN ('executive sponsor', 'vorgesetzter', 'leitung') THEN 'Executive Sponsor'
            ELSE NULL
        END AS "Role__c",

        -- Preferred Language mapping to target enum: (DE, EN, FR, ES, IT)
        CASE UPPER(TRIM(contact_raw.korrespondenzsprache))
            WHEN 'DE' THEN 'DE'
            WHEN 'GERMAN', 'DEU', 'DT' THEN 'DE'
            WHEN 'EN' THEN 'EN'
            WHEN 'ENGLISH', 'ENG' THEN 'EN'
            WHEN 'FR' THEN 'FR'
            WHEN 'FRENCH', 'FRE' THEN 'FR'
            WHEN 'ES' THEN 'ES'
            WHEN 'SPANISH', 'ESP' THEN 'ES'
            WHEN 'IT' THEN 'IT'
            WHEN 'ITALIAN', 'ITA' THEN 'IT'
            ELSE NULL
        END AS "Preferred_Language__c",

        -- AccountId: Must reference Salesforce-style Account Id, not raw source customer numbers
        -- Join to master_kunden and map kundennummer to Account.Id format
        '001' || RIGHT('0000000000' || mk.kundennummer, 10) AS "AccountId",

        -- Legacy contact ID from source natural key
        contact_raw.kontakt_id AS "Legacy_Contact_ID__c",

        -- Date fields: not present in source contacts table; use NULL per guidelines (prefer NULL over sentinel dates)
        NULL AS "CreatedDate",
        NULL AS "LastModifiedDate",

        -- Deleted flag: 0 = active, 1 = deleted
        0 AS "IsDeleted"

    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }} contact_raw
    INNER JOIN {{ source('fixture_master_v2_src', 'master_kunden') }} mk
        ON TRIM(contact_raw.kd_nummer) = TRIM(mk.kundennummer)

)

SELECT * FROM contacts