package entropydata.gcp;

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
import entropydata.sdk.EntropyDataAssetsProvider;
import entropydata.sdk.EntropyDataStateRepositoryInMemory;
import entropydata.sdk.client.model.Asset;
import entropydata.sdk.client.model.AssetColumnsInner;
import entropydata.sdk.client.model.AssetInfo;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcpAssetsProvider implements EntropyDataAssetsProvider {

  private static final Logger log = LoggerFactory.getLogger(GcpAssetsProvider.class);

  private final BigQuery bigquery;
  private final List<String> projectIds;
  private final EntropyDataStateRepositoryInMemory stateRepository;

  public GcpAssetsProvider(BigQuery bigquery, List<String> projectIds, EntropyDataStateRepositoryInMemory stateRepository) {
    this.bigquery = bigquery;
    this.projectIds = projectIds;
    this.stateRepository = stateRepository;
  }

  @Override
  public void fetchAssets(AssetCallback assetCallback) {

    final var gcpLastUpdatedAt = getLastUpdatedAt();
    var gcpLastUpdatedAtThisRunMax = gcpLastUpdatedAt;

    for(String projectId : projectIds) {
      log.info("Synchronizing project {}", projectId);

      Page<Dataset> datasetPage = bigquery.listDatasets(projectId, DatasetListOption.all());
      Iterable<Dataset> datasets = datasetPage.getValues();
      for(Dataset dataset : datasets) {
        try {
          log.info("Synchronizing dataset {}", dataset.getDatasetId());

          DatasetId datasetId = dataset.getDatasetId();

          Dataset datasetFull = bigquery.getDataset(datasetId);

          long gcpLastUpdatedDataset = getLastUpdated(datasetFull);
          if (gcpLastUpdatedDataset >= gcpLastUpdatedAt) {
            assetCallback.onAssetUpdated(toAsset(datasetFull));
          }

          Page<Table> tablePage = bigquery.listTables(datasetId, TableListOption.pageSize(1000L));

          Iterable<Table> tables = tablePage.getValues();
          for(Table table : tables) {
            log.info("Synchronizing table {}", table.getTableId());
            Table tableFull = bigquery.getTable(table.getTableId());

            long gcpLastUpdatedTable = getLastUpdated(tableFull);
            if (gcpLastUpdatedTable >= gcpLastUpdatedAt) {
              assetCallback.onAssetUpdated(toAsset(tableFull));
            }

            gcpLastUpdatedAtThisRunMax = Math.max(gcpLastUpdatedAtThisRunMax, gcpLastUpdatedTable);
          }
        } catch (Exception e) {
          log.warn("Failed to synchronize dataset {}: {}", dataset.getDatasetId(), e.getMessage());
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
