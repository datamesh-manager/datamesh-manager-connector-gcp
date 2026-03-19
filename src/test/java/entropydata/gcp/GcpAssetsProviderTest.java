package entropydata.gcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import entropydata.sdk.EntropyDataAssetsProvider.AssetCallback;
import entropydata.sdk.EntropyDataStateRepositoryInMemory;
import entropydata.sdk.client.model.Asset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class GcpAssetsProviderTest {

  private BigQuery bigQuery;
  private EntropyDataStateRepositoryInMemory stateRepository;
  private GcpAssetsProvider provider;
  private AssetCallback callback;

  @BeforeEach
  void setUp() {
    bigQuery = mock(BigQuery.class);
    stateRepository = new EntropyDataStateRepositoryInMemory("test-connector");
    provider = new GcpAssetsProvider(bigQuery, List.of("test-project"), stateRepository);
    callback = mock(AssetCallback.class);
  }

  /**
   * Create a simple Page implementation since Page.getValues() is a default method
   * that delegates to iterateAll() which we can't easily mock.
   */
  private static <T> Page<T> pageOf(List<T> items) {
    return new Page<T>() {
      @Override public boolean hasNextPage() { return false; }
      @Override public String getNextPageToken() { return null; }
      @Override public Page<T> getNextPage() { return null; }
      @Override public Iterable<T> iterateAll() { return items; }
      @Override public Iterable<T> getValues() { return items; }
    };
  }

  private Dataset mockDataset(String projectId, String datasetName, long lastModified) {
    var dataset = mock(Dataset.class);
    var datasetId = DatasetId.of(projectId, datasetName);
    when(dataset.getDatasetId()).thenReturn(datasetId);
    when(dataset.getGeneratedId()).thenReturn(projectId + ":" + datasetName);
    when(dataset.getFriendlyName()).thenReturn(datasetName);
    when(dataset.getLastModified()).thenReturn(lastModified);
    return dataset;
  }

  private Table mockTable(String projectId, String datasetName, String tableName, long lastModified,
      TableDefinition.Type type, Schema schema) {
    var table = mock(Table.class);
    var tableId = TableId.of(projectId, datasetName, tableName);
    when(table.getTableId()).thenReturn(tableId);
    when(table.getGeneratedId()).thenReturn(projectId + ":" + datasetName + "." + tableName);
    when(table.getFriendlyName()).thenReturn(tableName);
    when(table.getLastModifiedTime()).thenReturn(lastModified);

    var definition = mock(StandardTableDefinition.class);
    when(definition.getType()).thenReturn(type);
    when(definition.getSchema()).thenReturn(schema);
    when(table.getDefinition()).thenReturn(definition);

    return table;
  }

  @Test
  void syncsDatasetAsAsset() {
    var dataset = mockDataset("test-project", "my_dataset", 1000L);
    when(bigQuery.listDatasets(eq("test-project"), any(DatasetListOption.class)))
        .thenReturn(pageOf(List.of(dataset)));
    when(bigQuery.getDataset(dataset.getDatasetId())).thenReturn(dataset);
    when(bigQuery.listTables(eq(dataset.getDatasetId()), any(TableListOption.class)))
        .thenReturn(pageOf(List.of()));

    provider.fetchAssets(callback);

    var captor = ArgumentCaptor.forClass(Asset.class);
    verify(callback).onAssetUpdated(captor.capture());

    var asset = captor.getValue();
    assertThat(asset.getId()).isEqualTo("test-project:my_dataset");
    assertThat(asset.getInfo().getSource()).isEqualTo("gcp");
    assertThat(asset.getInfo().getType()).isEqualTo("dataset");
    assertThat(asset.getInfo().getStatus()).isEqualTo("active");
  }

  @Test
  void syncsTableWithSchemaAsAsset() {
    var dataset = mockDataset("test-project", "my_dataset", 1000L);
    when(bigQuery.listDatasets(eq("test-project"), any(DatasetListOption.class)))
        .thenReturn(pageOf(List.of(dataset)));
    when(bigQuery.getDataset(dataset.getDatasetId())).thenReturn(dataset);

    var schema = Schema.of(
        Field.of("id", LegacySQLTypeName.INTEGER),
        Field.newBuilder("name", LegacySQLTypeName.STRING).setDescription("Customer name").build()
    );
    var table = mockTable("test-project", "my_dataset", "customers", 2000L, TableDefinition.Type.TABLE, schema);
    when(bigQuery.listTables(eq(dataset.getDatasetId()), any(TableListOption.class)))
        .thenReturn(pageOf(List.of(table)));
    when(bigQuery.getTable(table.getTableId())).thenReturn(table);

    provider.fetchAssets(callback);

    var captor = ArgumentCaptor.forClass(Asset.class);
    verify(callback, org.mockito.Mockito.times(2)).onAssetUpdated(captor.capture());
    var assets = captor.getAllValues();

    var tableAsset = assets.stream()
        .filter(a -> "TABLE".equals(a.getInfo().getType()))
        .findFirst().orElseThrow();
    assertThat(tableAsset.getColumns()).hasSize(2);
    assertThat(tableAsset.getColumns().get(0).getName()).isEqualTo("id");
    assertThat(tableAsset.getColumns().get(0).getType()).isEqualTo("INTEGER");
    assertThat(tableAsset.getColumns().get(1).getName()).isEqualTo("name");
    assertThat(tableAsset.getColumns().get(1).getDescription()).isEqualTo("Customer name");
  }

  @Test
  void skipsAssetsNotModifiedSinceLastSync() {
    var dataset = mockDataset("test-project", "my_dataset", 1000L);
    when(bigQuery.listDatasets(eq("test-project"), any(DatasetListOption.class)))
        .thenReturn(pageOf(List.of(dataset)));
    when(bigQuery.getDataset(dataset.getDatasetId())).thenReturn(dataset);

    var table = mockTable("test-project", "my_dataset", "orders", 2000L, TableDefinition.Type.TABLE, null);
    when(bigQuery.listTables(eq(dataset.getDatasetId()), any(TableListOption.class)))
        .thenReturn(pageOf(List.of(table)));
    when(bigQuery.getTable(table.getTableId())).thenReturn(table);

    // First sync
    provider.fetchAssets(callback);
    verify(callback, org.mockito.Mockito.times(2)).onAssetUpdated(any());

    // Second sync - nothing changed
    var callback2 = mock(AssetCallback.class);
    provider.fetchAssets(callback2);

    // Table lastModified=2000 >= lastUpdatedAt=2000, so synced
    // Dataset lastModified=1000 < lastUpdatedAt=2000, so skipped
    verify(callback2, org.mockito.Mockito.atMost(1)).onAssetUpdated(any());
  }

  @Test
  void continuesOnDatasetPermissionError() {
    var dataset1 = mockDataset("test-project", "restricted_dataset", 1000L);
    var dataset2 = mockDataset("test-project", "accessible_dataset", 1000L);

    when(bigQuery.listDatasets(eq("test-project"), any(DatasetListOption.class)))
        .thenReturn(pageOf(List.of(dataset1, dataset2)));
    when(bigQuery.getDataset(dataset1.getDatasetId())).thenReturn(dataset1);
    when(bigQuery.getDataset(dataset2.getDatasetId())).thenReturn(dataset2);

    // First dataset throws permission error on table listing
    when(bigQuery.listTables(eq(dataset1.getDatasetId()), any(TableListOption.class)))
        .thenThrow(new com.google.cloud.bigquery.BigQueryException(403, "Permission denied"));
    when(bigQuery.listTables(eq(dataset2.getDatasetId()), any(TableListOption.class)))
        .thenReturn(pageOf(List.of()));

    provider.fetchAssets(callback);

    // Both datasets should still be synced as assets
    verify(callback, org.mockito.Mockito.atLeast(2)).onAssetUpdated(any());
  }

  @Test
  void handlesMultipleProjects() {
    var multiProjectProvider = new GcpAssetsProvider(bigQuery, List.of("project-a", "project-b"), stateRepository);

    var datasetA = mockDataset("project-a", "ds_a", 1000L);
    var datasetB = mockDataset("project-b", "ds_b", 1000L);

    when(bigQuery.listDatasets(eq("project-a"), any(DatasetListOption.class)))
        .thenReturn(pageOf(List.of(datasetA)));
    when(bigQuery.listDatasets(eq("project-b"), any(DatasetListOption.class)))
        .thenReturn(pageOf(List.of(datasetB)));
    when(bigQuery.getDataset(datasetA.getDatasetId())).thenReturn(datasetA);
    when(bigQuery.getDataset(datasetB.getDatasetId())).thenReturn(datasetB);
    when(bigQuery.listTables(any(DatasetId.class), any(TableListOption.class)))
        .thenReturn(pageOf(List.of()));

    multiProjectProvider.fetchAssets(callback);

    verify(callback, org.mockito.Mockito.times(2)).onAssetUpdated(any());
  }
}
