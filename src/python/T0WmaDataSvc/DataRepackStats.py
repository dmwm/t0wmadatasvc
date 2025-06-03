from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class RepackStats(RESTEntity):
  """REST entity for retrieving run/stream processing status."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('run', param, safe, RX_RUN, optional = False)

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self,run):
    """Check run mean repacking time

    :arg int run: the run number
    :returns: median and mean repacking time in hours"""

    sql = """select MEDIAN(time) as median, AVG(time) as mean 
                  from (SELECT (cast(t0_repacked_time as date) - cast(t0_checked_time as date))*24 as time
                        FROM file_transfer_status_offline
                        WHERE filename like '%'||:run||'%')
          """

    c, _ = self.api.execute(sql, run = run)

    runs=[]
    for result in c.fetchall():
      if result[0]:
        stats={'median' : result[0],
               'mean' : result[1]
               }
        runs.append(stats)

    return runs
