package entropydata.gcp;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.bigquery.BigQuery;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.context.ApplicationContext;

@SpringBootTest(properties = {
    "entropydata.client.host=http://localhost:0",
    "entropydata.client.apikey=test-key",
    "entropydata.client.gcp.accessmanagement.enabled=false",
    "entropydata.client.gcp.assets.enabled=false"
})
class ApplicationTest {

  @MockitoBean
  private BigQuery bigQuery;

  @Autowired
  private ApplicationContext context;

  @Autowired
  private GcpProperties gcpProperties;

  @Test
  void contextLoads() {
    assertThat(context).isNotNull();
  }

  @Test
  void propertiesAreBound() {
    assertThat(gcpProperties.accessmanagement().connectorid()).isEqualTo("gcp-access-management");
    assertThat(gcpProperties.accessmanagement().role()).isEqualTo("READER");
    assertThat(gcpProperties.accessmanagement().mapping().dataproduct().customfield()).isEqualTo("gcpPrincipal");
    assertThat(gcpProperties.accessmanagement().mapping().team().customfield()).isEqualTo("gcpPrincipal");
    assertThat(gcpProperties.assets().connectorid()).isEqualTo("gcp-asset-synchronizer");
    assertThat(gcpProperties.assets().projects()).containsExactly("test-project");
  }
}
