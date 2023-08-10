from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class DataExpressConfigHistory(RESTEntity):
  """REST entity for retrieving a specific primary dataset."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('stream', param, safe, RX_STREAM, optional = True)
    validate_str('scenario', param, safe, RX_SCENARIO, optional = True)
  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self, stream, scenario):
    """Retrieve Reco configuration and its history for a specific primary dataset

    :arg str stream: the stream name (optional, otherwise queries for all)
    :arg str scenario: scenario (optional, otherwise queries for all)
    :returns: stream, Scenario Acquisition era, minimum run, maximum run, CMSSW, PhysicsSkim, DqmSeq, GlobalTag"""

    sql = """
            SELECT express_config.stream p_stream, express_config.scenario p_scenario, MAX(run_config.run) max_run, MIN(run_config.run) min_run, express_config.cmssw cmssw, express_config.global_tag global_tag, express_config.alca_skim alca_skim, express_config.dqm_seq dqm_seq, run_config.acq_era acq_era
            FROM express_config
            JOIN run_config ON run_config.run = express_config.run
            """
    sql_with_primds = """
            WHERE stream = :p_stream
            GROUP BY run_config.acq_era, express_config.stream, express_config.scenario, express_config.cmssw,  express_config.global_tag, express_config.alca_skim, express_config.dqm_seq
            ORDER BY express_config.stream, MAX(run_config.run) desc, MIN(run_config.run) desc
            """
    sql_with_scenario = """
            WHERE express_config.scenario = :p_scenario
            GROUP BY run_config.acq_era, express_config.stream, express_config.scenario, express_config.cmssw,  express_config.global_tag, express_config.alca_skim, express_config.dqm_seq
            ORDER BY express_config.stream, MAX(run_config.run) desc, MIN(run_config.run) desc
            """
    sql_with_both = """
            WHERE express_config.stream = :p_stream AND express_config.scenario = :p_scenario
            GROUP BY run_config.acq_era, express_config.stream, express_config.scenario, express_config.cmssw,  express_config.global_tag, express_config.alca_skim, express_config.dqm_seq
            ORDER BY express_config.stream, MAX(run_config.run) desc, MIN(run_config.run) desc
            """
    sql_default = """
            GROUP BY run_config.acq_era, express_config.stream, express_config.scenario, express_config.cmssw,  express_config.global_tag, express_config.alca_skim, express_config.dqm_seq
            ORDER BY express_config.stream, MAX(run_config.run) desc, MIN(run_config.run) desc
            """   
    
    if stream is not None and scenario is None:
       sql_ = sql + sql_with_primds
       c, _ = self.api.execute(sql_, p_stream = stream)
    elif stream is not None and scenario is not None:
       sql_ = sql + sql_with_both
       c, _ = self.api.execute(sql_, p_stream = stream, p_scenario = scenario)
    elif stream is None and scenario is not None:
       sql_ = sql + sql_with_scenario
       c, _ = self.api.execute(sql_, p_scenario = scenario)
    else:
       sql_ = sql + sql_default
       c, _ = self.api.execute(sql_)

    configs = []
    for result in c.fetchall():

        (p_stream, p_scenario, max_run, min_run, cmssw, global_tag, alca_skim, dqm_seq, acq_era) = result

        config = { "stream" : p_stream,
                   "scenario" : p_scenario,
                   "max_run" : max_run,
                   "min_run" : min_run,
                   "cmssw" : cmssw,
                   "global_tag" : global_tag, 
                   "alca_skim" : alca_skim,
                   "dqm_seq" : dqm_seq,
                   "acq_era" : acq_era }
        configs.append(config)

    return configs
  
  