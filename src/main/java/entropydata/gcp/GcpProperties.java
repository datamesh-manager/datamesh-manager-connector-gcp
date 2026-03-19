package entropydata.gcp;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "entropydata.client.gcp")
public record GcpProperties(
    AccessmanagementProperties accessmanagement,
    AssetProperties assets
) {

  public record AccessmanagementProperties(
      String connectorid,
      Boolean enabled,
      String role,
      AccessmanagementMappingProperties mapping
  ) {
    public record AccessmanagementMappingProperties(
        AccessmanagementMappingCustomfieldProperties dataproduct,
        AccessmanagementMappingCustomfieldProperties team
    ) {
      public record AccessmanagementMappingCustomfieldProperties(
          String customfield
      ) {
      }
    }
  }

  public record AssetProperties(
      String connectorid,
      Boolean enabled,
      List<String> projects
  ) {
  }

}
