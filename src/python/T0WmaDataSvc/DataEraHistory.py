from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class EraHistory(RESTEntity):
  """REST entity for retrieving a specific primary dataset."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('era', param, safe, RX_ERA, optional = True)
  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self, era):
    """Retrieve Era history

    :arg str era: the acquisition era
    :returns: Acquisition era, minimum run, and maximum run"""

    sql_with_era = """
            SELECT acq_era, MAX(run) max_run, MIN(run) min_run
            FROM run_config
            WHERE acq_era LIKE :acq_era 
            GROUP BY acq_era
            ORDER BY max_run DESC, min_run DESC
            """

    sql_no_era = """
            SELECT acq_era, MAX(run) max_run, MIN(run) min_run
            FROM run_config
            GROUP BY acq_era
            ORDER BY max_run DESC, min_run DESC
            """
    

    if era is not None:
       c, _ = self.api.execute(sql_with_era, acq_era = '%' + str(era) + '%')
    else:
       c, _ = self.api.execute(sql_no_era)

    configs = []
    for result in c.fetchall():

        (acq_era, max_run, min_run) = result

        config = { "era" : acq_era,
                   "max_run" : max_run,
                   "min_run" : min_run }
        configs.append(config)

    return configs

  