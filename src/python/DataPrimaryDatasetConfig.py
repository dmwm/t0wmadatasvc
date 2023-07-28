from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class PrimaryDatasetConfig(RESTEntity):
  """REST entity for retrieving a specific primary dataset."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('primary_dataset', param, safe, RX_PRIMARY_DATASET, optional = True)

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self, primary_dataset):
    """Retrieve Reco configuration and its history for a specific primary dataset

    :arg str primary_dataset: the primary dataset name (optional, otherwise queries for muon 0)
    :returns: PrimaryDataset, Acquisition era, minimum run, maximum run, CMSSW, PhysicsSkim, DqmSeq, GlobalTag"""

    sql = """
            SELECT reco_config.primds primds, run_config.acq_era acq_era, MIN(run_config.run) min_run, MAX(run_config.run) max_run, reco_config.cmssw cmssw, reco_config.global_tag global_tag, reco_config.physics_skim physics_skim, reco_config.dqm_seq dqm_seq
            FROM reco_config
            JOIN run_config ON run_config.run = reco_config.run

            """
    sql_with_primds = "WHERE primds = :primary_dataset"
    sql_group = "GROUP BY run_config.acq_era, reco_config.primds, reco_config.cmssw,  reco_config.global_tag, reco_config.physics_skim, reco_config.dqm_seq"
    sql_order = "ORDER BY MIN(run_config.run) desc, MAX(run_config.run) desc"

    binds = {}
    if primary_dataset is not None:
        sql += sql_with_primds
        binds.update({"primds":primary_dataset})
    else:
       binds.update({"primds":'Muon0'})
       sql += "WHERE primds = 'Muon0'"

    sql += sql_group
    sql += sql_order
    sql += "INTO primary_dataset_config"
    c, _ = self.api.execute(sql, binds)
    results=c.fetchall()

  
    configs = []
    for primds, acq_era, min_run, max_run, cmssw, global_tag, physics_skim, dqm_seq in results:

        config = { "primary_dataset" : primds,
                   "acq_era" : acq_era,
                   "min_run" : min_run,
                   "max_run" : max_run,
                   "cmssw" : cmssw,
                   "global_tag" : global_tag,
                   "physics_skim" : physics_skim,
                   "dqm_seq" : dqm_seq
                   }
        configs.append(config)

    return configs
