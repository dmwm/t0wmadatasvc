from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class RecoConfig(RESTEntity):
  """REST entity for retrieving an specific run."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('run_id', param, safe, RX_RUNID, optional = True)
    validate_str('primary_dataset', param, safe, RX_PRIMARY_DATASET, optional = True)

  @restcall
  @tools.expires(secs=300)
  def get(self,run_id, primary_dataset):
    """Retrieve Express configuration from a specific run id.

    :arg int run_id: the run id number 
    :returns: CMSSW Release, Global Tag, Run number, Scenario"""

    sqlWhereWithRun="reco_config.run_id = :run_id"
    sqlWhereWithoutRun="reco_config.run_id = (select max(run_id) from reco_config)" #YOUNGEST RELEASED RUN THAT IS STILL RUNNING!!
    sqlWhereOptionprimaryDataset=" AND primary_dataset.name = :primary_dataset"

    sql = """SELECT reco_config.run_id AS run_id,
                                   primary_dataset.name AS primary_dataset,
                                   reco_config.proc_version AS proc_version,
                                   reco_config.global_tag AS global_tag,
                                   cmssw_version.name AS expversion
                            FROM reco_config
                            INNER JOIN primary_dataset ON
                              primary_dataset.id = reco_config.primds_id
                            INNER JOIN cmssw_version ON
                              cmssw_version.id = reco_config.cmssw_id
                            WHERE %s %s"""

    if run_id is not None and primary_dataset is None :
        c, _ = self.api.execute(sql % (sqlWhereWithRun, ''), run_id = run_id)
    elif run_id is not None and primary_dataset is not None :
        c, _ = self.api.execute(sql % (sqlWhereWithRun, sqlWhereOptionprimaryDataset), run_id = run_id, primary_dataset = primary_dataset)
    else :
        c, _ = self.api.execute(sql % (sqlWhereWithoutRun, ''))

    primaryDatasets = []
    for primaryDataset in c.fetchall():
        (run, primaryDataset, processingVersion, globalTag, release) = primaryDataset

        primaryDatasetDict = { "cmssw_version"   :   release,
                                 "run"       :   run,
                                 "proc_version" :   processingVersion,
                                 "global_tag"   :   globalTag,
                                 "primary_dataset"       :   primaryDataset }
        primaryDatasets.append(primaryDatasetDict)

    return str(primaryDatasets)
