from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class RunStreamSkippedLumis(RESTEntity):
  """REST entity for retrieving run/stream/lumis that have been skipped during processing."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('run', param, safe, RX_RUN, optional = True)
    validate_str('stream', param, safe, RX_STREAM, optional = True)

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self,run, stream):
    """Retrieve run/stream/lumis that have been skipped during processing. If no run or stream is 
    specified, returns the latest 100 skipped runs

    :arg int run: the run number
    :arg str primary_dataset: the primary dataset name (optional, otherwise queries for all)
    :returns: list of skipped lumis in each run/stream combination, and number of events in the lumi"""

    sql = """SELECT run, stream, lumi, events
             FROM skipped_streamers
             WHERE run is not null"""
    sqlWithRun = " AND skipped_streamers.run = :run"
    sqlWithStream = " AND skipped_streamers.stream = :stream"
    sqlOrder = " ORDER BY run desc, stream asc, lumi asc"
    sqlLimit = " FETCH FIRST 100 ROWS ONLY"

    binds = {}
    if run is not None:
        sql += sqlWithRun
        binds.update({"run":run})
    if stream is not None:
        sql += sqlWithStream
        binds.update({"stream":stream})
    sql += sqlOrder
    if run is None:
        sql += sqlLimit

    c, _ = self.api.execute(sql, binds)
    results=c.fetchall()

    runs={}
    for run, stream, lumi, events in results:
        runDict=runs.setdefault(run,{})
        streamDict=runDict.setdefault(stream,{})
        streamDict[lumi]=events

    return [runs]