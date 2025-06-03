from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class PromptRecoStatus(RESTEntity):
  """REST entity for retrieving an specific run."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data. In this case there is no input data."""

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self):
    """Retrieve PromptReco status

    :returns: PrompReco Status: True(Enable) or False(Disabled)"""

    sql = """SELECT status
             FROM promptreco_status
             WHERE change_time =
               (SELECT MAX(change_time) FROM promptreco_status)"""

    c, _ = self.api.execute(sql)

    return [ c.fetchall()[0][0] == 1 ]
