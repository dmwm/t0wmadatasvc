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
    validate_str('primary_dataset', param, safe, RX_PRIMARY_DATASET, optional = False)

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self, primary_dataset):
    """Retrieve Reco configuration and its history for a specific primary dataset

    :arg str primary_dataset: the primary dataset name (optional, otherwise queries for muon 0)
    :returns: PrimaryDataset, Acquisition era, minimum run, maximum run, CMSSW, PhysicsSkim, DqmSeq, GlobalTag"""

    sql = """
            SELECT reco_config.primds primds, MIN(run_config.run) min_run, MAX(run_config.run) max_run, reco_config.cmssw cmssw, reco_config.global_tag global_tag, reco_config.physics_skim physics_skim, reco_config.dqm_seq dqm_seq, run_config.acq_era acq_era
            FROM reco_config
            JOIN run_config ON run_config.run = reco_config.run
            WHERE primds = :primds
            GROUP BY run_config.acq_era, reco_config.primds, reco_config.cmssw,  reco_config.global_tag, reco_config.physics_skim, reco_config.dqm_seq
            ORDER BY MIN(run_config.run) desc, MAX(run_config.run) desc
            """
    
    c, _ = self.api.execute(sql, primds = primary_dataset)

    configs = []
    for result in c.fetchall():

        (primds, min_run, max_run, cmssw, global_tag, physics_skim, dqm_seq, acq_era) = result

        config = { "primary_dataset" : primds,
                   "min_run" : min_run,
                   "max_run" : max_run,
                   "cmssw" : cmssw,
                   "global_tag" : global_tag, 
                   "physics_skim" : physics_skim,
                   "dqm_seq" : dqm_seq,
                   "acq_era" : acq_era }
        configs.append(config)

    return configs
  