App workflows are requested by B2B apps
B2B apps are registered with out app as apps with permission to drive our workflows via API request.
lifecycle workflow

1. B2B app submits via platform/submit (platform/ will be the endpoints exposed to B2B app in our funciton app gateway where auth lives) -> returns a request ID
2. B2B app polls platform/status/<requestID> and gets updates on processing status- once complete it returns a bunch of job results but most importantly are preview URLs and an approval UI 
3. B2B app uses either platform/approve or the approval UI (which uses platform/approve) to approve or reject 
4. upon approval the relevant service URLs are shared (ending in version=latest)- if this is a semantic version update optional URL with version=previous_version will also be created and version=latest is updated with the newest data


B2B app has unique identifiers internal to its own system and optionally a semantic version identifier so for example
B2B App named "Data Explorer" uses DatasetID + AppID + VersionID as its unique identifier- we don't care about the distinction between DatasetID and AppID (internal too B2B app) we will combine them to make our own uuid and since this B2B app uses semantic versining we will track that with VersionID. 
This means new dataset like:
DatasetID = birds2023
AppID = birdviewerapp
VersionID = 1

-> our app creates a completely new record with a uuid hash of birds2023+birdviewerapp, new ETL, new service API layer URLs ending version=latest

next submission:
DatasetID = birds2023
AppID = birdviewerapp
VersionID = 2
PreviousVersionID (will be a mandtatory parameter) = 1

-> our app recognizes a semantic version update and creates new ETL, new service API URls within the existing record, URLS ending version=latest updated to now point to version 2 data, optionally creating version=1 URLs for old versions

Our first class entity to handle all this will be a GeospatialAsset (until we find a better name) and:
GeospatialAsset unique ID is a hash of B2B App name + (B2B unique ids)
GeospatialAsset has a property indicating if it has semantic versions (almost always will)
GeospatialAsset has multiple states (mainted in the database state tables)
1. ApprovalState - Pending | Approved | Rejected | Revoked
2. ClassificationState - Official Use Only (OUO) | Public
3. Processing state - Queued | Processing | Completed | Failed | TBD if others
