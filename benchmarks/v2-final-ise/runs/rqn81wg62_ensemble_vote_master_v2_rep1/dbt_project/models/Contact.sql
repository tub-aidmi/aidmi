WITH contacts AS (
    SELECT
        -- Id: transform kontakt_id "CON-00001" → keep format but ensure consistent numeric padding
        'CON-' || LPAD(SUBSTRING(kontakt_id FROM '\d+'), 5, '0') AS "Id",

        -- FirstName from rufname
        INITCAP(TRIM(rufname)) AS "FirstName",

        -- LastName from familienname (NOT NULL in target, default to 'Unknown' if empty)
        CASE 
            WHEN TRIM(familienname) IS NULL OR TRIM(familienname) = '' THEN 'Unknown'
            ELSE INITCAP(TRIM(familienname))
        END AS "LastName",

        -- Email from kontakt_email (lowercase, trimmed)
        LOWER(TRIM(kontakt_email)) AS "Email",

        -- Phone: clean formatting by removing parentheses and spaces
        CASE
            WHEN TRIM(tel) IS NULL OR TRIM(tel) = '' OR TRIM(UPPER(tel)) = 'N/A' THEN NULL
            ELSE REPLACE(REPLACE(TRANSLATE(tel, '()', ''), ' ', ''), '-', '')
        END AS "Phone",

        -- Title (job title / occupation) from berufsbezeichnung
        CASE 
            WHEN TRIM(berufsbezeichnung) IS NULL OR TRIM(berufsbezeichnung) = '' THEN NULL
            ELSE INITCAP(TRIM(berufsbezeichnung))
        END AS "Title",

        -- Role__c: normalize German and English role values into target enum
        CASE 
            WHEN LOWER(TRIM(COALESCE(rolle, ''))) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
            WHEN LOWER(TRIM(COALESCE(rolle, ''))) IN ('end user', 'endanwender', 'n/a')  THEN 'End User'
            WHEN LOWER(TRIM(COALESCE(rolle, ''))) IN ('technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
            WHEN LOWER(TRIM(COALESCE(rolle, ''))) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
            ELSE NULL
        END AS "Role__c",

        -- Preferred_Language__c: normalize multi-language language names to ISO-2 codes
        CASE 
            WHEN LOWER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('english', 'englisch', 'en')                        THEN 'EN'
            WHEN LOWER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('deutsch', 'german', 'de')                        THEN 'DE'
            WHEN LOWER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('french', 'französisch', 'fr')                   THEN 'FR'
            WHEN LOWER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('spanish', 'español', 'it', 'italiano', 'italian')  THEN 'ES'
            WHEN LOWER(TRIM(COALESCE(korrespondenzsprache, ''))) IN ('italiano', 'italian')                             THEN 'IT'
            ELSE NULL
        END AS "Preferred_Language__c",

        -- AccountId: transform kd_nummer from "CUST-M1001" → "ACC-M1001" to match Account.Id format
        'ACC-' || SUBSTRING(kd_nummer FROM 6) AS "AccountId",

        -- Legacy_Contact_ID__c: store original source key for traceability
        kontakt_id AS "Legacy_Contact_ID__c",

        -- CreatedDate: not available in source, set to NULL
        NULL::TEXT AS "CreatedDate",

        -- LastModifiedDate: not available in source, set to NULL
        NULL::TEXT AS "LastModifiedDate",

        -- IsDeleted: default 0 (not deleted)
        0 AS "IsDeleted"

    FROM {{ source('fixture_master_v2_src', 'master_kontakte') }}
)
SELECT * FROM contacts
WHERE "LastName" != 'Unknown'  -- exclude rows where both name fields are empty/NULL