from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class ExpressConfig(RESTEntity):
  """REST entity for retrieving an specific run."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('run_id', param, safe, RX_RUNID, optional = True)
    validate_str('stream', param, safe, RX_STREAM, optional = True)


  @restcall
  @tools.expires(secs=300)
  def get(self,run_id, stream):
    """Retrieve Express configuration from a specific run id.

    :arg int run_id: the run id number 
    :returns: CMSSW Release, Global Tag, Run number, Scenario"""

    sqlWhereWithRun="express_config.run_id = :run_id"
    sqlWhereWithoutRun="express_config.run_id = (select max(run_id) from express_config)"
    sqlWhereOptionStream=" AND stream.name = :stream"

    sql = """SELECT express_config.run_id AS run_id,
                                   stream.name AS stream,
                                   express_config.proc_version AS proc_version,
                                   express_config.global_tag AS global_tag,
                                   cmssw_version.name AS expversion,
                                   event_scenario.name AS scenario
                            FROM express_config
                            INNER JOIN stream ON
                              stream.id = express_config.stream_id
                            INNER JOIN run_stream_cmssw_assoc ON
                              run_stream_cmssw_assoc.run_id = express_config.run_id AND
                              run_stream_cmssw_assoc.stream_id = express_config.stream_id
                            INNER JOIN cmssw_version ON
                              cmssw_version.id = run_stream_cmssw_assoc.override_version
                            INNER JOIN stream_special_primds_assoc ON
                              stream_special_primds_assoc.stream_id = express_config.stream_id
                            INNER JOIN run_primds_scenario_assoc ON
                              run_primds_scenario_assoc.run_id = express_config.run_id AND
                              run_primds_scenario_assoc.primds_id = stream_special_primds_assoc.primds_id
                            INNER JOIN event_scenario ON
                              event_scenario.id = run_primds_scenario_assoc.scenario_id
                            WHERE %s %s"""

    if run_id is not None and stream is None :
        c, _ = self.api.execute(sql % (sqlWhereWithRun, ''), run_id = run_id)
    elif run_id is not None and stream is not None :
        c, _ = self.api.execute(sql % (sqlWhereWithRun, sqlWhereOptionStream), run_id = run_id, stream = stream)
    else :
        c, _ = self.api.execute(sql % (sqlWhereWithoutRun, ''))

    streams = []
    for stream in c.fetchall():
        (run, stream, processingVersion, globalTag, release, scenario) = stream

        streamDict = { "expversion"   :   release,
                     "run_id"       :   run,
                     "scenario"     :   scenario,
                     "proc_version" :   processingVersion,
                     "global_tag"   :   globalTag,
                     "stream"       :   stream }
        streams.append(streamDict)

    return str(streams)
