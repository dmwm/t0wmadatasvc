from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class RecoConfigHistory(RESTEntity):
  """REST entity for retrieving a specific primary dataset."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('primary_dataset', param, safe, RX_PRIMARY_DATASET, optional = True)
    validate_str('scenario', param, safe, RX_SCENARIO, optional = True)
  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self, primary_dataset, scenario):
    """Retrieve Reco configuration and its history for a specific primary dataset

    :arg str primary_dataset: the primary dataset name (optional, otherwise queries for all)
    :arg str scenario: scenario (optional, otherwise queries for all)
    :returns: PrimaryDataset, Scenario, Acquisition era, minimum run, maximum run, CMSSW, PhysicsSkim, DqmSeq, GlobalTag"""

    sql = """
            SELECT reco_config.primds primds, reco_config.scenario p_scenario, MAX(run_config.run) max_run, MIN(run_config.run) min_run, reco_config.cmssw cmssw, reco_config.global_tag global_tag, reco_config.physics_skim physics_skim, reco_config.dqm_seq dqm_seq, run_config.acq_era acq_era
            FROM reco_config
            JOIN run_config ON run_config.run = reco_config.run
            """
    sql_with_primds = """
            WHERE primds = :primds
            GROUP BY run_config.acq_era, reco_config.primds, reco_config.scenario, reco_config.cmssw,  reco_config.global_tag, reco_config.physics_skim, reco_config.dqm_seq
            ORDER BY reco_config.primds, MAX(run_config.run) desc, MIN(run_config.run) desc
            """
    sql_with_scenario = """
            WHERE reco_config.scenario = :p_scenario
            GROUP BY run_config.acq_era, reco_config.primds, reco_config.scenario, reco_config.cmssw,  reco_config.global_tag, reco_config.physics_skim, reco_config.dqm_seq
            ORDER BY reco_config.primds, MAX(run_config.run) desc, MIN(run_config.run) desc
            """
    sql_with_both = """
            WHERE reco_config.primds = :primds AND reco_config.scenario = :p_scenario
            GROUP BY run_config.acq_era, reco_config.primds, reco_config.scenario, reco_config.cmssw,  reco_config.global_tag, reco_config.physics_skim, reco_config.dqm_seq
            ORDER BY reco_config.primds, MAX(run_config.run) desc, MIN(run_config.run) desc
            """
    sql_default = """
            GROUP BY run_config.acq_era, reco_config.primds, reco_config.scenario, reco_config.cmssw,  reco_config.global_tag, reco_config.physics_skim, reco_config.dqm_seq
            ORDER BY reco_config.primds, MAX(run_config.run) desc, MIN(run_config.run) desc
            """   
    
    if primary_dataset is not None and scenario is None:
       sql_ = sql + sql_with_primds
       c, _ = self.api.execute(sql_, primds = primary_dataset)
    elif primary_dataset is not None and scenario is not None:
       sql_ = sql + sql_with_both
       c, _ = self.api.execute(sql_, primds = primary_dataset, p_scenario = scenario)
    elif primary_dataset is None and scenario is not None:
       sql_ = sql + sql_with_scenario
       c, _ = self.api.execute(sql_, p_scenario = scenario)
    else:
       sql_ = sql + sql_default
       c, _ = self.api.execute(sql_)

    configs = []
    for result in c.fetchall():

        (primds, p_scenario, max_run, min_run, cmssw, global_tag, physics_skim, dqm_seq, acq_era) = result

        config = { "primary_dataset" : primds,
                   "scenario" : p_scenario,
                   "max_run" : max_run,
                   "min_run" : min_run,
                   "cmssw" : cmssw,
                   "global_tag" : global_tag, 
                   "physics_skim" : physics_skim,
                   "dqm_seq" : dqm_seq,
                   "acq_era" : acq_era }
        configs.append(config)

    return configs

  