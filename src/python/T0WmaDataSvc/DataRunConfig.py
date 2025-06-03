from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class RunConfig(RESTEntity):
  """REST entity for retrieving an specific run."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('run', param, safe, RX_RUN, optional = True)

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self,run):
    """Retrieve Run configuration for a specific run

    :arg int run: the run number (latest if not specified)
    :returns: Run, AcquisitionEra"""

    sqlWhereWithRun="run_config.run = :run"
    sqlWhereWithoutRun="run_config.run = (select max(run) from run_config)"

    sql = """SELECT run_config.run,
                    run_config.acq_era
             FROM run_config
             WHERE %s"""

    if run is not None :
        c, _ = self.api.execute(sql % sqlWhereWithRun, run = run)
    else :
        c, _ = self.api.execute(sql % sqlWhereWithoutRun)

    configs = []
    for result in c.fetchall():

      (run, acq_era) = result

      config = { "run" : run,
                 "acq_era" : acq_era }
      configs.append(config)

    return configs
