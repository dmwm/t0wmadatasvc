from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class RunDatasetDone(RESTEntity):
  """REST entity for retrieving an specific run."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('run', param, safe, RX_RUN, optional = False)
    validate_str('primary_dataset', param, safe, RX_PRIMARY_DATASET, optional = True)

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self,run, primary_dataset):
    """Retrieve PromptReco completion status for a given run (and primary dataset)

    :arg int run: the run number
    :arg str primary_dataset: the primary dataset name (optional, otherwise queries for aggregation of all)
    :returns: True or False"""

    sqlWithoutDataset = """SELECT NVL(MIN(finished), 0)
                           FROM run_primds_done
                           WHERE run = :run"""

    sqlWithDataset = """SELECT NVL(MIN(finished), 0)
                        FROM run_primds_done
                        WHERE run = :run
                        AND primds = :primds"""

    if primary_dataset is None :
        c, _ = self.api.execute(sqlWithoutDataset, run = run)
    else:
        c, _ = self.api.execute(sqlWithDataset, run = run, primds = primary_dataset)

    return [ c.fetchall()[0][0] == 1 ]
