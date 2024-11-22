package datameshmanager.gcp;

import com.google.cloud.bigquery.Acl;
import com.google.cloud.bigquery.Acl.Entity;
import com.google.cloud.bigquery.Acl.Group;
import com.google.cloud.bigquery.Acl.User;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import datameshmanager.sdk.DataMeshManagerClient;
import datameshmanager.sdk.DataMeshManagerEventHandler;
import datameshmanager.sdk.client.model.Access;
import datameshmanager.sdk.client.model.AccessActivatedEvent;
import datameshmanager.sdk.client.model.AccessDeactivatedEvent;
import datameshmanager.sdk.client.model.DataProduct;
import datameshmanager.sdk.client.model.Team;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcpAccessManagement implements DataMeshManagerEventHandler {

  private static final Logger log = LoggerFactory.getLogger(GcpAccessManagement.class);

  private final DataMeshManagerClient client;
  private final BigQuery bigQuery;

  private final String teamCustomField;
  private final String dataProductCustomField;
  private final String role;

  public GcpAccessManagement(DataMeshManagerClient client, BigQuery bigQuery, String role, String teamCustomField, String dataProductCustomField) {
    this.client = client;
    this.bigQuery = bigQuery;
    this.role = role;
    this.teamCustomField = teamCustomField;
    this.dataProductCustomField = dataProductCustomField;
  }

  @Override
  public void onAccessActivatedEvent(AccessActivatedEvent event) {
    String accessId = event.getId();
    log.info("Processing AccessActivatedEvent {}", accessId);
    var access = client.getAccessApi().getAccess(accessId);

    var datasetId = findProviderDatasetId(access, client);
    if (datasetId == null) {
      return;
    }

    var entity = findConsumerEntity(access, client);
    if (entity == null) {
      return;
    }

    authorize(datasetId, entity);

    addTag(accessId, "permission-granted-on-gcp");
  }

  @Override
  public void onAccessDeactivatedEvent(AccessDeactivatedEvent event) {
    String accessId = event.getId();
    log.info("Processing AccessDeactivatedEvent {}", accessId);
    var access = client.getAccessApi().getAccess(accessId);

    var datasetId = findProviderDatasetId(access, client);
    if (datasetId == null) {
      return;
    }

    var entity = findConsumerEntity(access, client);
    if (entity == null) {
      return;
    }

    deauthorize(datasetId, entity);

    removeTag(accessId, "permission-granted-on-gcp");
  }

  public void authorize(DatasetId datasetId, Entity entity) {
    var dataset = bigQuery.getDataset(datasetId);
    if (dataset == null) {
      log.info("Cannot authorize as dataset {} does not exist", datasetId);
      return;
    }

    List<Acl> aclList = dataset.getAcl() == null ? new ArrayList<>() : new ArrayList<>(dataset.getAcl());
    var expectedRole = Acl.Role.valueOf(role);
    for (var acl : aclList) {
      boolean isAlreadyGranted = acl.getRole().equals(expectedRole) && acl.getEntity().equals(entity);
      if (isAlreadyGranted) {
        log.info("Already authorized entity {} with role {} for dataset {}", entity, expectedRole, datasetId);
        return;
      }
    }

    Acl acl = Acl.of(entity, expectedRole);
    aclList.add(acl);
    dataset.toBuilder().setAcl(aclList).build().update();
    log.info("Authorized entity {} with role {} for dataset {} ", entity, expectedRole, datasetId);
  }

  public void deauthorize(DatasetId datasetId, Entity entity) {
    var dataset = bigQuery.getDataset(datasetId);

    List<Acl> aclList = dataset.getAcl() == null ? new ArrayList<>() : new ArrayList<>(dataset.getAcl());
    var expectedRole = Acl.Role.valueOf(role);
    Acl matchedAcl = null;
    for (var acl : aclList) {
      boolean matchingAcl = acl.getRole().equals(expectedRole) && acl.getEntity().equals(entity);
      if (matchingAcl) {
        matchedAcl = acl;
      }
    }
    if (matchedAcl == null) {
      log.info("Already deauthorized entity {} with role {} for dataset {}", entity, expectedRole, datasetId);
      return;
    }

    aclList.remove(matchedAcl);

    dataset.toBuilder().setAcl(aclList).build().update();
    log.info("Deauthorized entity {} with role {} for dataset {} ", entity, expectedRole, datasetId);
  }

  private Entity findConsumerEntity(Access access, DataMeshManagerClient client) {
    if (access.getConsumer() == null) {
      log.debug("Abort, as no consumer is available");
      return null;
    }

    var dataProductId = access.getConsumer().getDataProductId();
    if (dataProductId != null) {
      var dataProduct = client.getDataProductsApi().getDataProduct(dataProductId);
      return getEntityForDataProduct(dataProduct);
    }

    var teamId = access.getConsumer().getTeamId();
    if (teamId != null) {
      var team = client.getTeamsApi().getTeam(teamId);
      return getEntityForTeam(team);
    }

    var userId = access.getConsumer().getUserId();
    if (userId != null) {
      // requires https://cloud.google.com/iam/docs/principal-identifiers#v1
      // user:USER_EMAIL_ADDRESS
      // userId is always the email address at the moment
      return new User(userId);
    }

    return null;
  }

  private Entity getEntityForDataProduct(DataProduct dataProduct) {
    var gcpServiceAccount = getCustom(dataProduct.getCustom(), dataProductCustomField);
    if (gcpServiceAccount != null && gcpServiceAccount.startsWith("serviceAccount:")) {
      // requires https://cloud.google.com/iam/docs/principal-identifiers#v1
      // serviceAccount:SA_EMAIL_ADDRESS
      return new User(gcpServiceAccount.substring("serviceAccount:".length()));
    }

    return null;
  }

  private Group getEntityForTeam(Team team) {
    var gcpGroup = getCustom(team.getCustom(), teamCustomField);
    if (gcpGroup != null && gcpGroup.startsWith("group:")) {
      // requires https://cloud.google.com/iam/docs/principal-identifiers#v1
      // group:GROUP_EMAIL_ADDRESS
      return new Group(gcpGroup.substring("group:".length()));
    }

    return null;
  }

  private static String getCustom(Map<String, String> custom, String key) {
    if (custom != null) {
      var gcpGroup = custom.get(key);
      if (gcpGroup != null) {
        return gcpGroup;
      }
    }
    return null;
  }

  private static DatasetId findProviderDatasetId(Access access, DataMeshManagerClient client) {
    var provider = access.getProvider();
    if (provider == null) {
      log.debug("Abort, as no provider is available");
      return null;
    }

    var providerDataProduct = client.getDataProductsApi()
        .getDataProduct(provider.getDataProductId());

    var outputPorts = providerDataProduct.getOutputPorts();
    if (outputPorts == null) {
      log.debug("Abort, as no output port is available");
      return null;
    }
    var providerOutputPort = outputPorts.stream().filter(it -> it.getId().equals(provider.getOutputPortId())).findFirst().orElse(null);
    if (providerOutputPort == null) {
      log.debug("Abort, as no output port found for given output port id");
      return null;
    }

    var server = providerOutputPort.getServer();
    if (server == null) {
      log.debug("Abort, as no server is available");
      return null;
    }

    var serverDataset = server.getDataset();
    var serverProject = server.getProject();

    if (serverProject == null) {
      log.debug("Abort, as no project is available");
      return null;
    }

    if (serverDataset == null) {
      log.debug("Abort, as no dataset is available");
      return null;
    }

    return DatasetId.of(serverProject, serverDataset);
  }

  private void removeTag(String accessId, String tag) {
    Access access = client.getAccessApi().getAccess(accessId);
    if (access.getTags() != null) {
      access.getTags().remove(tag);
    }
    client.getAccessApi().addAccess(access.getId(), access);
  }

  private void addTag(String accessId, String tag) {
    Access access = client.getAccessApi().getAccess(accessId);
    if (access.getTags() == null) {
      access.setTags(new ArrayList<>());
    }
    access.getTags().add(tag);
    client.getAccessApi().addAccess(access.getId(), access);
  }

}
