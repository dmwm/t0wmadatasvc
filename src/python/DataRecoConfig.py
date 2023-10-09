from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class RecoConfig(RESTEntity):
  """REST entity for retrieving an specific run."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('run', param, safe, RX_RUN, optional = True)
    validate_str('primary_dataset', param, safe, RX_PRIMARY_DATASET, optional = True)

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self,run, primary_dataset):
    """Retrieve Reco configuration for a specific run (and primary dataset)

    :arg int run: the run number (latest if not specified)
    :arg str primary_dataset: the primary dataset name (optional, otherwise queries for all)
    :returns: Run, PrimaryDataset, CMSSW, ScramArch, AlcaSkim, PhysicsSkim, DqmSeq, GlobalTag, Scenario and Processing Version"""

    sqlWhereWithRun="reco_config.run = :run"
    sqlWhereWithoutRun="reco_config.run = (select max(run) from reco_config)"
    sqlWhereOptionPrimaryDataset=" AND reco_config.primds = :primds"

    sql = """SELECT reco_config.run,
                    reco_config.primds,
                    reco_config.cmssw,
                    reco_config.scram_arch,
                    reco_config.alca_skim,
                    reco_config.physics_skim,
                    reco_config.dqm_seq,
                    reco_config.global_tag,
                    reco_config.scenario,
                    reco_config.proc_version,
                    reco_config.multicore,
                    reco_config.write_reco,
                    reco_config.write_dqm,
                    reco_config.write_aod,
                    reco_config.write_miniaod,
                    reco_config.write_nanoaod
             FROM reco_config
             WHERE %s %s"""

    if run is not None and primary_dataset is None :
        c, _ = self.api.execute(sql % (sqlWhereWithRun, ''), run = run)
    elif run is not None and primary_dataset is not None :
        c, _ = self.api.execute(sql % (sqlWhereWithRun, sqlWhereOptionPrimaryDataset), run = run, primds = primary_dataset)
    else :
        c, _ = self.api.execute(sql % (sqlWhereWithoutRun, ''))

    configs = []
    for result in c.fetchall():

        (run, primds, cmssw, scram_arch, alca_skim, physics_skim, dqm_seq,
         global_tag, scenario, proc_version, multicore, write_reco, write_dqm, write_aod, write_miniaod, write_nanoaod) = result

        config = { "run" : run,
                   "primary_dataset" : primds,
                   "cmssw" : cmssw,
                   "scram_arch" : scram_arch,
                   "alca_skim" : alca_skim,
                   "physics_skim" : physics_skim,
                   "dqm_seq" : dqm_seq,
                   "global_tag" : global_tag,
                   "scenario" : scenario,
                   "proc_version" : proc_version,
                   "multicore" : multicore,
                   "write_reco": bool(write_reco),
                   "write_dqm" : bool(write_dqm),
                   "write_aod" : bool(write_aod),
                   "write_miniaod" : bool(write_miniaod),
                   "write_nanoaod" : bool(write_nanoaod) }
        configs.append(config)

    return configs
