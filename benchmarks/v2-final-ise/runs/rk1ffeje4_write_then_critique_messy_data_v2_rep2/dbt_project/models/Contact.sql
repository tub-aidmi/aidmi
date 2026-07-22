{{ config(materialized='table') }}

SELECT
    CAST(c.id AS TEXT) AS "Id",
    INITCAP(TRIM(c.firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(c.lastname)), 'Unknown Contact') AS "LastName",
    CASE
        WHEN TRIM(c.email) IN ('', 'N/A') THEN NULL
        ELSE TRIM(c.email)
    END AS "Email",
    c.phone AS "Phone",
    INITCAP(TRIM(c.title)) AS "Title",
    CASE LOWER(REGEXP_REPLACE(TRIM(COALESCE(c.role__c, '')), '\s+', ' ', 'g'))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'ende anwender' THEN 'End User'
        WHEN 'endanwender' THEN 'End User'
        WHEN 'entscheider' THEN 'Decision Maker'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'technischer ansprechpartner' THEN 'Technical Contact'
        WHEN 'techniker' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        WHEN 'sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE LOWER(REGEXP_REPLACE(TRIM(COALESCE(c.preferred_language__c, '')), '\s+', ' ', 'g'))
        WHEN 'de' THEN 'DE'
        WHEN 'deutsch' THEN 'DE'
        WHEN 'german' THEN 'DE'
        WHEN 'en' THEN 'EN'
        WHEN 'englisch' THEN 'EN'
        WHEN 'english' THEN 'EN'
        WHEN 'fr' THEN 'FR'
        WHEN 'français' THEN 'FR'
        WHEN 'französisch' THEN 'FR'
        WHEN 'french' THEN 'FR'
        WHEN 'es' THEN 'ES'
        WHEN 'español' THEN 'ES'
        WHEN 'spanisch' THEN 'ES'
        WHEN 'spanish' THEN 'ES'
        WHEN 'it' THEN 'IT'
        WHEN 'italiano' THEN 'IT'
        WHEN 'italienisch' THEN 'IT'
        WHEN 'italian' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CAST(a.id AS TEXT) AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON c.accountid = a.id