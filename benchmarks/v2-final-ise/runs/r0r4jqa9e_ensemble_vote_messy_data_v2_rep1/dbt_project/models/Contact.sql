{{ config(materialized='table') }}

SELECT
    -- Id: Source contact id → Target Id (primary key)
    "id" AS "Id",

    -- FirstName: Normalize with INITCAP, trim whitespace
    INITCAP(TRIM("firstname")) AS "FirstName",

    -- LastName: NOT NULL constraint — default to 'Unknown' if null/empty
    COALESCE(NULLIF(INITCAP(TRIM("lastname")), ''), 'Unknown') AS "LastName",

    -- Email: convert "N/A" and empty strings to NULL; keep valid emails as-is
    CASE WHEN TRIM(COALESCE("email", '')) IN ('N/A', '') THEN NULL ELSE "email" END AS "Email",

    -- Phone: preserve as-is (no transformation needed per spec)
    "phone" AS "Phone",

    -- Title: normalize with INITCAP and trim
    INITCAP(TRIM("title")) AS "Title",

    -- Role__c: map German/English/source variants to target enum values
    CASE UPPER(TRIM(COALESCE("role__c", '')))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER'      THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN 'ENTSCHEIDER'   THEN 'Decision Maker'
        WHEN 'ENDANWENDER'   THEN 'End User'
        WHEN 'TECHNISCHER ANSPRECHPARTNER' THEN 'Technical Contact'
        WHEN 'TECHNIKER'     THEN 'Technical Contact'
        WHEN 'SPONSOR'       THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",

    -- Preferred_Language__c: map language names to ISO 2-letter codes (DE, EN, FR)
    CASE UPPER(TRIM(COALESCE("preferred_language__c", '')))
        WHEN 'DE'          THEN 'DE'
        WHEN 'DEUTSCH'     THEN 'DE'
        WHEN 'EN'          THEN 'EN'
        WHEN 'ENGLISCH'    THEN 'EN'
        WHEN 'ENGLISH'     THEN 'EN'
        WHEN 'FR'          THEN 'FR'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        WHEN 'FRENCH'      THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",

    -- AccountId: source accountid is already in CUST-XXXX format matching Account.id — no transform needed
    "accountid" AS "AccountId",

    -- Legacy_Contact_ID__c: source natural key
    "id" AS "Legacy_Contact_ID__c",

    -- Audit / Salesforce fields not present in source — use NULL defaults
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0    AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'contact') }}