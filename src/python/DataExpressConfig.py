from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class ExpressConfig(RESTEntity):
  """REST entity for retrieving an specific run."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('run', param, safe, RX_RUN, optional = True)
    validate_str('stream', param, safe, RX_STREAM, optional = True)

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self,run, stream):
    """Retrieve Express configuration for a specific run (and stream)

    :arg int run: the run number (latest if not specified)
    :arg str stream: the stream name (optional, otherwise queries for all)
    :returns: Run, Stream, CMSSW, ScramArch, AlcaSkims, DqmSeq, GlobalTag, Scenario"""

    sqlWhereWithRun="express_config.run = :run"
    sqlWhereWithoutRun="express_config.run = (select max(run) from express_config)"
    sqlWhereOptionStream=" AND express_config.stream = :stream"

    sql = """SELECT express_config.run,
                    express_config.stream,
                    express_config.cmssw,
                    express_config.scram_arch,
                    express_config.reco_cmssw,
                    express_config.reco_scram_arch,
                    express_config.alca_skim,
                    express_config.dqm_seq,
                    express_config.global_tag,
                    express_config.scenario,
                    express_config.proc_version,
                    express_config.multicore,
                    express_config.write_tiers,
                    express_config.write_dqm
             FROM express_config
             WHERE %s %s"""

    if run is not None and stream is None :
        c, _ = self.api.execute(sql % (sqlWhereWithRun, ''), run = run)
    elif run is not None and stream is not None :
        c, _ = self.api.execute(sql % (sqlWhereWithRun, sqlWhereOptionStream), run = run, stream = stream)
    else :
        c, _ = self.api.execute(sql % (sqlWhereWithoutRun, ''))

    configs = []
    for result in c.fetchall():

        (run, stream, cmssw, scram_arch, reco_cmssw, reco_scram_arch, alca_skim,
         dqm_seq, global_tag, scenario, proc_version, multicore, write_tiers, write_dqm) = result

        config = { "run" : run,
                   "stream" : stream,
                   "cmssw" : cmssw,
                   "scram_arch" : scram_arch,
                   "reco_cmssw" : reco_cmssw,
                   "reco_scram_arch" : reco_scram_arch,
                   "alca_skim" : alca_skim,
                   "dqm_seq" : dqm_seq,
                   "global_tag" : global_tag,
                   "scenario" : scenario,
                   "proc_version" : proc_version,
                   "multicore" : multicore,
                   "write_tiers" : write_tiers,
                   "write_dqm" : bool(write_dqm) }
        configs.append(config)

    return configs
