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
            SELECT run_config.acq_era, MAX(run_config.run) max_run, MIN(run_config.run) min_run, dbsbuffer_pending_blocks.pending_blocks
            FROM run_config
            INNER JOIN dbsbuffer_pending_blocks ON dbsbuffer_pending_blocks.acq_era = run_config.acq_era
            WHERE run_config.acq_era LIKE :acq_era 
            GROUP BY run_config.acq_era
            ORDER BY max_run DESC, min_run DESC
            """

    sql_no_era = """
            SELECT run_config.acq_era acq_era, MAX(run_config.run) max_run, MIN(run_config.run) min_run, dbsbuffer_pending_blocks.pending_blocks pending_blocks
            FROM run_config
            INNER JOIN dbsbuffer_pending_blocks ON dbsbuffer_pending_blocks.acq_era = run_config.acq_era
            GROUP BY acq_era
            ORDER BY max_run DESC, min_run DESC
            """
    

    if era is not None:
       c, _ = self.api.execute(sql_with_era, acq_era = '%' + str(era) + '%')
    else:
       c, _ = self.api.execute(sql_no_era)

    configs = []
    for result in c.fetchall():

        (acq_era, max_run, min_run, pending_blocks) = result
        if pending_blocks != 0:
          config = { "era" : acq_era,
                    "max_run" : max_run,
                    "min_run" : min_run,
                    "pending_blocks" : pending_blocks }
        else:
          config = { "era" : acq_era,
                    "max_run" : max_run,
                    "min_run" : min_run }
                    
        configs.append(config)

    return configs

  