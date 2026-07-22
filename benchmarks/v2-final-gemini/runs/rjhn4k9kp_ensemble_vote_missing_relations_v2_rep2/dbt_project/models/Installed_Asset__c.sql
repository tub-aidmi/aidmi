SELECT
    s_asset.id AS "Id",
    COALESCE(s_asset.name, 'Unknown Asset') AS "Name",
    s_asset.serial AS "Serial_Number__c",
    CASE
        WHEN s_asset.warranty ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_CHAR(TO_DATE(s_asset.warranty, 'YYYY-MM-DD'), 'YYYY-MM-DD')
        ELSE NULL
    END AS "Warranty_End_Date__c",
    COALESCE(acc_id.id, acc_name.id) AS "Account__c",
    s_project.id AS "Project__c",
    s_asset.id AS "Legacy_Asset_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'asset') }} AS s_asset
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc_id
    ON s_asset.client = acc_id.id
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS acc_name
    ON s_asset.client = acc_name.name
    AND acc_id.id IS NULL -- Only join by name if no match by ID was found
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'project') }} AS s_project
    ON s_asset.project = s_project.id