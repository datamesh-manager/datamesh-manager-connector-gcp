package datameshmanager.gcp;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.resourcemanager.v3.ProjectsClient;
import datameshmanager.sdk.DataMeshManagerAssetsSynchronizer;
import datameshmanager.sdk.DataMeshManagerClient;
import datameshmanager.sdk.DataMeshManagerEventListener;
import datameshmanager.sdk.DataMeshManagerStateRepositoryInMemory;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@SpringBootApplication(scanBasePackages = "datameshmanager")
@ConfigurationPropertiesScan("datameshmanager")
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Bean
  public BigQuery bigQuery() {
    return BigQueryOptions.getDefaultInstance().getService();
  }

  @Bean
  public ProjectsClient projectsClient() {
    try {
      return ProjectsClient.create();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Bean
  public DataMeshManagerClient dataMeshManagerClient(@Value("${datameshmanager.client.host}") String host,
      @Value("${datameshmanager.client.apikey}") String apiKey) {
    return new DataMeshManagerClient(host, apiKey);
  }

  @Bean(initMethod = "start", destroyMethod = "stop")
  @ConditionalOnProperty(value = "datameshmanager.client.gcp.accessmanagement.enabled", havingValue = "true")
  public DataMeshManagerEventListener dataMeshManagerEventListener(DataMeshManagerClient client, GcpProperties gcpProperties,
      BigQuery bigQuery, TaskExecutor taskExecutor) {
    var connectorid = gcpProperties.accessmanagement().connectorid();
    var stateRepository = new DataMeshManagerStateRepositoryInMemory(connectorid);
    var eventHandler = new GcpAccessManagement(client, bigQuery, gcpProperties.accessmanagement().role(),
        gcpProperties.accessmanagement().mapping().team().customfield(),
        gcpProperties.accessmanagement().mapping().dataproduct().customfield());
    var listener = new DataMeshManagerEventListener(connectorid, "accessmanagement", client, eventHandler, stateRepository);
    taskExecutor.execute(listener::start);
    return listener;
  }

  @Bean(initMethod = "start", destroyMethod = "stop")
  @ConditionalOnProperty(value = "datameshmanager.client.gcp.asset.enabled", havingValue = "true")
  public DataMeshManagerAssetsSynchronizer dataMeshManagerAssetsSynchronizer(DataMeshManagerClient client, GcpProperties gcpProperties,
      BigQuery bigQuery, TaskExecutor taskExecutor, ProjectsClient projectsClient) {
    var connectorid = gcpProperties.assets().connectorid();
    var stateRepository = new DataMeshManagerStateRepositoryInMemory(connectorid);
    var assetsProvider = new GcpAssetsProvider(bigQuery, projectsClient, stateRepository);
    var assetsSynchronizer = new DataMeshManagerAssetsSynchronizer(connectorid, client, assetsProvider);
    taskExecutor.execute(assetsSynchronizer::start);
    return assetsSynchronizer;
  }

  @Bean
  public TaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(5);
    executor.setMaxPoolSize(10);
    executor.setQueueCapacity(25);
    executor.setThreadNamePrefix("datameshmanager-connector-");
    executor.initialize();
    return executor;
  }

}
