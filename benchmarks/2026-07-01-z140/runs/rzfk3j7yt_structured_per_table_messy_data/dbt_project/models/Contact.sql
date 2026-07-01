SELECT 
    CAST(Id AS text) AS "Id",
    TRIM(FirstName) AS "FirstName",
    TRIM(LastName) AS "LastName",
    LOWER(TRIM(Email)) AS "Email",
    TRIM(Phone) AS "Phone",
    INITCAP(TRIM(Title)) AS "Title",
    CASE 
        WHEN UPPER(TRIM(COALESCE(Role__c, ''))) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(COALESCE(Role__c, ''))) = 'END USER' THEN 'End User'
        WHEN UPPER(TRIM(COALESCE(Role__c, ''))) = 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN UPPER(TRIM(COALESCE(Role__c, ''))) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN UPPER(TRIM(COALESCE(Preferred_Language__c, ''))) IN ('DE', 'DEUTSCH') THEN 'DE'
        WHEN UPPER(TRIM(COALESCE(Preferred_Language__c, ''))) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(TRIM(COALESCE(Preferred_Language__c, ''))) = 'FR' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(AccountId) AS "AccountId",
    NULL::text AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_src', 'Contact') }}