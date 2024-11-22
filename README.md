Data Mesh Manager Agent for GCP
===

The agent for GCP is a Spring Boot application that uses the [datamesh-manager-sdk](https://github.com/datamesh-manager/datamesh-manager-sdk) internally, and is available as a ready-to-use Docker image [datameshmanager/datamesh-manager-agent-gcp](https://hub.docker.com/repository/docker/datameshmanager/datamesh-manager-gcp-databricks) to be deployed in your environment.

## Features

- **Asset Synchronization**: Sync tables and datasets of BigQuery projects to the Data Mesh Manager as Assets.
- **Access Management**: Listen for AccessActivated and AccessDeactivated events in the Data Mesh Manager and grants access on BigQuery datasets to the data consumer.

## Usage

Start the agent using Docker. You must pass the API keys as environment variables.

```
docker run \
  -e DATAMESHMANAGER_CLIENT_APIKEY='insert-api-key-here' \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/filename.json \
  -v $GOOGLE_APPLICATION_CREDENTIALS:/tmp/keys/filename.json:ro \
  datameshmanager/datamesh-manager-agent-gcp:latest
```

## Configuration

## Configuration

| Environment Variable                                                         | Default Value                      | Description                                                                            |
|------------------------------------------------------------------------------|------------------------------------|----------------------------------------------------------------------------------------|
| `DATAMESHMANAGER_CLIENT_HOST`                                                | `https://api.datamesh-manager.com` | Base URL of the Data Mesh Manager API.                                                 |
| `DATAMESHMANAGER_CLIENT_APIKEY`                                              |                                    | API key for authenticating requests to the Data Mesh Manager.                          |
| `DATAMESHMANAGER_CLIENT_GCP_ACCESSMANAGEMENT_AGENTID`                 | `gcp-access-management`            | Identifier for the GCP access management agent.                                 |
| `DATAMESHMANAGER_CLIENT_GCP_ACCESSMANAGEMENT_ENABLED`                 | `true`                             | Indicates whether GCP access management is enabled.                             |
| `DATAMESHMANAGER_CLIENT_GCP_ACCESSMANAGEMENT_MAPPING_DATAPRODUCT_CUSTOMFIELD` | `gcpPrincipal`                     | Custom field mapping for GCP service principals in data products.               |
| `DATAMESHMANAGER_CLIENT_GCP_ACCESSMANAGEMENT_MAPPING_TEAM_CUSTOMFIELD`       | `gcpPrincipal`                     | Custom field mapping for GCP service principals in teams.                       |
| `DATAMESHMANAGER_CLIENT_GCP_ASSETS_AGENTID`                           | `gcp-assets`                       | Identifier for the GCP assets agent.                                            |
| `DATAMESHMANAGER_CLIENT_GCP_ASSETS_ENABLED`                           | `true`                             | Indicates whether GCP asset tracking is enabled.                                |
| `DATAMESHMANAGER_CLIENT_GCP_ASSETS_POLLINTERVAL`                      | `PT5S`                             | Polling interval for GCP asset updates, in ISO 8601 duration format.            |
| `DATAMESHMANAGER_CLIENT_GCP_ASSETS_TABLES_ALLOWLIST`                  | `*`                                | List of allowed tables for GCP asset tracking (wildcard `*` allows all tables). |