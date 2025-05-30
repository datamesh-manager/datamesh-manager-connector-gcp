package datameshmanager.gcp;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datameshmanager.client.gcp")
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
      Boolean enabled
  ) {
  }

}
