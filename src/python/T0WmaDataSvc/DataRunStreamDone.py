from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class RunStreamDone(RESTEntity):
  """REST entity for retrieving run/stream processing status."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('run', param, safe, RX_RUN, optional = False)
    validate_str('stream', param, safe, RX_STREAM, optional = False)

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self,run, stream):
    """Check run/stream processing status

    :arg int run: the run number
    :arg str stream: the stream name
    :returns: True or False"""

    sql = """SELECT COUNT(*)
             FROM run_stream_done
             WHERE run_stream_done.run = :run
             AND run_stream_done.stream = :stream"""

    c, _ = self.api.execute(sql, run = run, stream = stream)

    return [ c.fetchall()[0][0] == 1 ]
