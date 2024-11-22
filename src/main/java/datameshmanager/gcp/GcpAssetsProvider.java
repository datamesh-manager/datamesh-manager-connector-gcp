package datameshmanager.gcp;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.resourcemanager.v3.ListProjectsRequest;
import com.google.cloud.resourcemanager.v3.Project;
import com.google.cloud.resourcemanager.v3.ProjectsClient;
import com.google.cloud.resourcemanager.v3.ProjectsClient.ListProjectsPage;
import com.google.cloud.resourcemanager.v3.ProjectsClient.ListProjectsPagedResponse;
import datameshmanager.sdk.DataMeshManagerAssetsProvider;
import datameshmanager.sdk.DataMeshManagerStateRepositoryInMemory;
import datameshmanager.sdk.client.model.Asset;
import datameshmanager.sdk.client.model.AssetColumnsInner;
import datameshmanager.sdk.client.model.AssetInfo;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcpAssetsProvider implements DataMeshManagerAssetsProvider {

  private static final Logger log = LoggerFactory.getLogger(GcpAssetsProvider.class);

  private final BigQuery bigquery;
  private final ProjectsClient projectsClient;
  private final DataMeshManagerStateRepositoryInMemory stateRepository;

  public GcpAssetsProvider(BigQuery bigquery, ProjectsClient projectsClient, DataMeshManagerStateRepositoryInMemory stateRepository) {
    this.bigquery = bigquery;
    this.projectsClient = projectsClient;
    this.stateRepository = stateRepository;
  }

  @Override
  public void fetchAssets(AssetCallback assetCallback) {

    final var gcpLastUpdatedAt = getLastUpdatedAt();
    var gcpLastUpdatedAtThisRunMax = gcpLastUpdatedAt;

    ListProjectsPagedResponse response = projectsClient.listProjects(ListProjectsRequest.getDefaultInstance());
    ListProjectsPage page = response.getPage();
    List<Project> projects = page.getResponse().getProjectsList();
    for(Project project : projects) {
      String projectId = project.getProjectId();
      log.info("Synchronizing project {}", projectId);

      Page<Dataset> datasetPage = bigquery.listDatasets(projectId, DatasetListOption.all());
      Iterable<Dataset> datasets = datasetPage.getValues();
      for(Dataset dataset : datasets) {
        log.info("Synchronizing dataset {}", dataset.getDatasetId());

        DatasetId datasetId = dataset.getDatasetId();

        Dataset datasetFull = bigquery.getDataset(datasetId);

        long gcpLastUpdatedDataset = getLastUpdated(datasetFull);
        if (gcpLastUpdatedAt > gcpLastUpdatedDataset) {
          assetCallback.onAssetUpdated(toAsset(datasetFull));
        }

        Page<Table> tablePage = bigquery.listTables(datasetId, TableListOption.pageSize(1000L));

        Iterable<Table> tables = tablePage.getValues();
        for(Table table : tables) {
          log.info("Synchronizing table {}", table.getTableId());
          Table tableFull = bigquery.getTable(table.getTableId());

          long gcpLastUpdatedTable = getLastUpdated(tableFull);
          if (gcpLastUpdatedAt > gcpLastUpdatedTable) {
            assetCallback.onAssetUpdated(toAsset(tableFull));
          }

          gcpLastUpdatedAtThisRunMax = Math.max(gcpLastUpdatedAtThisRunMax, gcpLastUpdatedTable);
        }
      }
    }

    setLastUpdatedAt(gcpLastUpdatedAtThisRunMax);
  }

  private long getLastUpdated(Table table) {
    return table.getLastModifiedTime();
  }

  private long getLastUpdated(Dataset dataset) {
    return dataset.getLastModified();
  }

  private Asset toAsset(Table table) {
    Asset asset = new Asset()
        .id(table.getGeneratedId())
        .info(new AssetInfo()
            .name(table.getFriendlyName())
            .source("gcp")
            .qualifiedName(table.getTableId().toString())
            .status("active")
            .description(table.getDescription()))
        .putPropertiesItem("updatedAt", table.getLastModifiedTime().toString());

    if (table.getDefinition() != null) {
      TableDefinition tableDefinition = table.getDefinition();
      AssetInfo info = asset.getInfo();
      if (info != null) {
        info.type(tableDefinition.getType().name());
      }
      Schema schema = tableDefinition.getSchema();
      if (schema != null) {
        FieldList fields = schema.getFields();
        if (fields != null) {
          for (Field field : fields) {
            asset.addColumnsItem(new AssetColumnsInner()
                .name(field.getName())
                .type(field.getType().name())
                .description(field.getDescription()));
          }
        }
      }
    }

    return asset;
  }

  private static Asset toAsset(Dataset dataset) {
    return new Asset()
        .id(dataset.getGeneratedId())
        .info(new AssetInfo()
            .name(dataset.getFriendlyName())
            .source("gcp")
            .qualifiedName(dataset.getDatasetId().toString())
            .type("dataset")
            .status("active")
            .description(dataset.getDescription()))
        .putPropertiesItem("updatedAt", dataset.getLastModified().toString());
  }

  private Long getLastUpdatedAt() {
    Map<String, Object> state = stateRepository.getState();
    return (Long) state.getOrDefault("lastUpdatedAt", 0L);
  }

  private void setLastUpdatedAt(Long gcpLastUpdatedAtThisRunMax) {
    Map<String, Object> state = Map.of("lastUpdatedAt", gcpLastUpdatedAtThisRunMax);
    stateRepository.saveState(state);
  }
}
