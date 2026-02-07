# Azure Setup for DAG Orchestrator

**EPOCH:** 5 - DAG ORCHESTRATION
**CREATED:** 29 JAN 2026
**STATUS:** Production Ready

## Overview

This document covers the Azure configuration required for the DAG Orchestrator (`rmhdagmaster`). It includes both:
- **Service Request Templates** for QA/Production (where you need admin assistance)
- **Azure CLI Commands** for Dev environments (where you are your own admin)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DAG ORCHESTRATOR ARCHITECTURE                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐     │
│  │   rmhdagmaster  │      │   Service Bus   │      │  Docker Workers │     │
│  │   (Web App)     │─────▶│   rmhazure      │◀─────│  (rmhheavyapi)  │     │
│  │   Orchestrator  │      │ dag-worker-tasks│      │                 │     │
│  └────────┬────────┘      └─────────────────┘      └─────────────────┘     │
│           │                                                                 │
│           │ UMI: rmhpgflexadmin                                             │
│           ▼                                                                 │
│  ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐     │
│  │   PostgreSQL    │      │   rmhazuregeo   │      │  rmhstorage123  │     │
│  │   rmhpostgres   │      │   (Bronze)      │      │  (Silver)       │     │
│  │   dagapp schema │      │                 │      │                 │     │
│  └─────────────────┘      └─────────────────┘      └─────────────────┘     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

| Resource | Name | Purpose |
|----------|------|---------|
| Web App | `rmhdagmaster` | Orchestrator container |
| Managed Identity | `rmhpgflexadmin` | Authentication (UMI) |
| PostgreSQL | `rmhpostgres.postgres.database.azure.com` | Database |
| Service Bus | `rmhazure.servicebus.windows.net` | Task dispatch |
| Storage (Bronze) | `rmhazuregeo` | Raw uploads |
| Storage (Silver) | `rmhstorage123` | Processed data |

---

## 1. Managed Identity Assignment

### Service Request Template (QA/Prod)

```
Subject: Assign User Managed Identity to Web App

Request:
  Assign the existing User Managed Identity "rmhpgflexadmin" to Web App "rmhdagmaster"

  Identity Details:
    Name: rmhpgflexadmin
    Client ID: a533cb80-a590-4fad-8e52-1eb1f72659d7
    Resource Group: rmhazure_rg

  Web App Details:
    Name: rmhdagmaster
    Resource Group: rmhazure_rg

  This identity will be used for:
    - PostgreSQL authentication (Entra ID)
    - Storage account access
    - Service Bus access
```

### Azure CLI (Dev)

```bash
# Check current identity assignment
az webapp identity show --name rmhdagmaster --resource-group rmhazure_rg

# Assign user-managed identity to web app
az webapp identity assign \
  --name rmhdagmaster \
  --resource-group rmhazure_rg \
  --identities /subscriptions/fc7a176b-9a1d-47eb-8a7f-08cc8058fcfa/resourcegroups/rmhazure_rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/rmhpgflexadmin

# Verify assignment
az webapp identity show --name rmhdagmaster --resource-group rmhazure_rg \
  --query "userAssignedIdentities" -o json
```

---

## 2. Storage Account RBAC

### Service Request Template (QA/Prod)

```
Subject: Grant Storage Access for DAG Orchestrator

Request:
  Assign "Storage Blob Data Contributor" role to managed identity on storage accounts.

  Identity:
    Name: rmhpgflexadmin
    Principal ID: ab45e154-ae11-4e99-9e96-76da5fe51656

  Storage Accounts:
    1. rmhazuregeo (Bronze - raw uploads)
       Role: Storage Blob Data Contributor

    2. rmhstorage123 (Silver - processed data)
       Role: Storage Blob Data Contributor

  Justification:
    Orchestrator needs to pass blob paths in workflow parameters and may
    need to verify file existence. Workers will do the actual file processing.
```

### Azure CLI (Dev)

```bash
# Get principal ID for the managed identity
PRINCIPAL_ID=$(az identity show --name rmhpgflexadmin --resource-group rmhazure_rg --query principalId -o tsv)
echo "Principal ID: $PRINCIPAL_ID"

# Grant Storage Blob Data Contributor on Bronze storage (rmhazuregeo)
az role assignment create \
  --assignee $PRINCIPAL_ID \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/fc7a176b-9a1d-47eb-8a7f-08cc8058fcfa/resourceGroups/rmhazure_rg/providers/Microsoft.Storage/storageAccounts/rmhazuregeo"

# Grant Storage Blob Data Contributor on Silver storage (rmhstorage123)
az role assignment create \
  --assignee $PRINCIPAL_ID \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/fc7a176b-9a1d-47eb-8a7f-08cc8058fcfa/resourceGroups/rmhazure_rg/providers/Microsoft.Storage/storageAccounts/rmhstorage123"

# Verify assignments
az role assignment list --assignee $PRINCIPAL_ID \
  --query "[].{role:roleDefinitionName,scope:scope}" -o table
```

---

## 3. Service Bus RBAC

### Service Request Template (QA/Prod)

```
Subject: Grant Service Bus Access for DAG Orchestrator

Request:
  Assign "Azure Service Bus Data Sender" role to managed identity.

  Identity:
    Name: rmhpgflexadmin
    Principal ID: ab45e154-ae11-4e99-9e96-76da5fe51656

  Service Bus:
    Namespace: rmhazure
    Role: Azure Service Bus Data Sender
    Scope: Entire namespace (all queues)

  Justification:
    Orchestrator dispatches tasks to workers via Service Bus queues.
    Needs SENDER permission to enqueue task messages.
    Does NOT need Receiver permission (workers receive tasks).
```

### Azure CLI (Dev)

```bash
PRINCIPAL_ID="ab45e154-ae11-4e99-9e96-76da5fe51656"

# Grant Azure Service Bus Data Sender on namespace
az role assignment create \
  --assignee $PRINCIPAL_ID \
  --role "Azure Service Bus Data Sender" \
  --scope "/subscriptions/fc7a176b-9a1d-47eb-8a7f-08cc8058fcfa/resourceGroups/rmhazure_rg/providers/Microsoft.ServiceBus/namespaces/rmhazure"

# Verify
az role assignment list --assignee $PRINCIPAL_ID \
  --query "[?contains(scope, 'ServiceBus')]" -o table
```

---

## 4. Service Bus Queue Creation

### Service Request Template (QA/Prod)

```
Subject: Create Service Bus Queue for DAG Orchestrator

Request:
  Create a new queue in the rmhazure Service Bus namespace.

  Queue Details:
    Name: dag-worker-tasks
    Namespace: rmhazure
    Message TTL: 1 day (P1D)
    Max Size: 1024 MB
    Lock Duration: 1 minute
    Max Delivery Count: 10
    Dead-lettering: Enabled on expiration

  Purpose:
    Queue for dispatching DAG workflow tasks to worker containers.
```

### Azure CLI (Dev)

```bash
# Create the queue
az servicebus queue create \
  --name dag-worker-tasks \
  --namespace-name rmhazure \
  --resource-group rmhazure_rg \
  --default-message-time-to-live P1D \
  --max-size 1024 \
  --lock-duration PT1M \
  --max-delivery-count 10 \
  --enable-dead-lettering-on-message-expiration true

# Verify queue exists
az servicebus queue list \
  --namespace-name rmhazure \
  --resource-group rmhazure_rg \
  --query "[].name" -o tsv
```

---

## 5. PostgreSQL User Creation

### Service Request Template (QA/Prod)

```
Subject: Create PostgreSQL AAD User for DAG Orchestrator

Request:
  Create a PostgreSQL user for Entra ID (AAD) authentication.

  Identity:
    Name: rmhpgflexadmin
    Type: User Managed Identity

  Database:
    Server: rmhpostgres.postgres.database.azure.com
    Database: postgres

  SQL to Execute (as admin):
    -- Create the AAD principal
    SELECT * FROM pgaadauth_create_principal('rmhpgflexadmin', false, false);

    -- Create the dagapp schema
    CREATE SCHEMA IF NOT EXISTS dagapp;

    -- Grant permissions
    GRANT ALL ON SCHEMA dagapp TO "rmhpgflexadmin";
    GRANT ALL ON ALL TABLES IN SCHEMA dagapp TO "rmhpgflexadmin";
    GRANT ALL ON ALL SEQUENCES IN SCHEMA dagapp TO "rmhpgflexadmin";
    ALTER DEFAULT PRIVILEGES IN SCHEMA dagapp GRANT ALL ON TABLES TO "rmhpgflexadmin";
    ALTER DEFAULT PRIVILEGES IN SCHEMA dagapp GRANT ALL ON SEQUENCES TO "rmhpgflexadmin";

  Verification:
    SELECT * FROM pgaadauth_list_principals() WHERE rolname = 'rmhpgflexadmin';
```

### Azure CLI + psql (Dev)

```bash
# Get an access token for PostgreSQL
TOKEN=$(az account get-access-token --resource-type oss-rdbms --query accessToken -o tsv)

# Connect with token
PGPASSWORD=$TOKEN psql \
  "host=rmhpostgres.postgres.database.azure.com port=5432 dbname=postgres user=rob634@rmhpostgres sslmode=require" \
  -c "SELECT * FROM pgaadauth_create_principal('rmhpgflexadmin', false, false);"

# Or use Azure Portal Query Editor
```

---

## 6. Web App Environment Variables

### Service Request Template (QA/Prod)

```
Subject: Configure Environment Variables for DAG Orchestrator

Request:
  Set the following application settings on Web App "rmhdagmaster":

  Authentication:
    USE_MANAGED_IDENTITY=true
    AZURE_CLIENT_ID=a533cb80-a590-4fad-8e52-1eb1f72659d7
    DB_ADMIN_MANAGED_IDENTITY_NAME=rmhpgflexadmin

  Database:
    POSTGRES_HOST=rmhpostgres.postgres.database.azure.com
    POSTGRES_DB=postgres
    POSTGRES_PORT=5432
    AUTO_BOOTSTRAP_SCHEMA=true

  Storage:
    BRONZE_STORAGE_ACCOUNT=rmhazuregeo
    SILVER_STORAGE_ACCOUNT=rmhstorage123

  Service Bus:
    SERVICE_BUS_FQDN=rmhazure.servicebus.windows.net
    DAG_WORKER_QUEUE=dag-worker-tasks

  Application:
    ORCHESTRATOR_POLL_INTERVAL=1.0
    LOG_LEVEL=INFO
```

### Azure CLI (Dev)

```bash
az webapp config appsettings set \
  --name rmhdagmaster \
  --resource-group rmhazure_rg \
  --settings \
  "USE_MANAGED_IDENTITY=true" \
  "AZURE_CLIENT_ID=a533cb80-a590-4fad-8e52-1eb1f72659d7" \
  "DB_ADMIN_MANAGED_IDENTITY_NAME=rmhpgflexadmin" \
  "POSTGRES_HOST=rmhpostgres.postgres.database.azure.com" \
  "POSTGRES_DB=postgres" \
  "POSTGRES_PORT=5432" \
  "AUTO_BOOTSTRAP_SCHEMA=true" \
  "BRONZE_STORAGE_ACCOUNT=rmhazuregeo" \
  "SILVER_STORAGE_ACCOUNT=rmhstorage123" \
  "SERVICE_BUS_FQDN=rmhazure.servicebus.windows.net" \
  "DAG_WORKER_QUEUE=dag-worker-tasks" \
  "ORCHESTRATOR_POLL_INTERVAL=1.0" \
  "LOG_LEVEL=INFO"

# Verify settings
az webapp config appsettings list \
  --name rmhdagmaster \
  --resource-group rmhazure_rg \
  -o table
```

---

## 7. Verification Checklist

### Azure CLI Verification Script

```bash
#!/bin/bash
# verify_setup.sh - Verify DAG Orchestrator Azure configuration

echo "=== DAG Orchestrator Setup Verification ==="

# 1. Check web app exists and is running
echo -e "\n1. Web App Status:"
az webapp show --name rmhdagmaster --resource-group rmhazure_rg \
  --query "{name:name,state:state,defaultHostName:defaultHostName}" -o table

# 2. Check identity assignment
echo -e "\n2. Managed Identity:"
az webapp identity show --name rmhdagmaster --resource-group rmhazure_rg \
  --query "userAssignedIdentities" -o json | head -10

# 3. Check RBAC roles
echo -e "\n3. RBAC Role Assignments:"
az role assignment list --assignee ab45e154-ae11-4e99-9e96-76da5fe51656 \
  --query "[].{role:roleDefinitionName,resource:scope}" -o table

# 4. Check Service Bus queue
echo -e "\n4. Service Bus Queues:"
az servicebus queue list --namespace-name rmhazure --resource-group rmhazure_rg \
  --query "[].{name:name,status:status}" -o table

# 5. Check environment variables
echo -e "\n5. Key Environment Variables:"
az webapp config appsettings list --name rmhdagmaster --resource-group rmhazure_rg \
  --query "[?contains(name, 'POSTGRES') || contains(name, 'MANAGED') || contains(name, 'SERVICE_BUS')].name" -o tsv

echo -e "\n=== Verification Complete ==="
```

---

## Current State (29 JAN 2026)

| Component | Status | Notes |
|-----------|--------|-------|
| Web App (`rmhdagmaster`) | ✅ Running | |
| UMI Assignment | ✅ Configured | `rmhpgflexadmin` assigned |
| Storage RBAC (Bronze) | ✅ Configured | `rmhazuregeo` |
| Storage RBAC (Silver) | ✅ Configured | `rmhstorage123` |
| Service Bus RBAC | ✅ Configured | Data Sender role |
| Service Bus Queue | ✅ Created | `dag-worker-tasks` |
| Environment Variables | ✅ Set | All required vars configured |
| PostgreSQL User | ⚠️ Verify | May need to create if not exists |
| Container Deployment | ⏳ Pending | Need to push image to ACR |

---

## Troubleshooting

### Token Acquisition Failures

```bash
# Test token acquisition manually
az account get-access-token --resource https://ossrdbms-aad.database.windows.net/.default

# Check identity can get tokens
az login --identity --username a533cb80-a590-4fad-8e52-1eb1f72659d7
```

### Database Connection Issues

```bash
# Verify AAD principal exists
psql -c "SELECT * FROM pgaadauth_list_principals() WHERE rolname = 'rmhpgflexadmin';"

# Check schema permissions
psql -c "\dn+ dagapp"
```

### Service Bus Issues

```bash
# Check queue is receiving messages
az servicebus queue show \
  --name dag-worker-tasks \
  --namespace-name rmhazure \
  --resource-group rmhazure_rg \
  --query "{messageCount:countDetails.activeMessageCount,deadLetter:countDetails.deadLetterMessageCount}"
```
