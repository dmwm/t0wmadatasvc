from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class FirstConditionSafeRun(RESTEntity):
  """REST entity for retrieving an specific run."""

  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data. In this case there is no input data"""

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self):
    """Latest run which has not released PromptReco yet

    :returns: Latest run which has not released PromptReco yet"""

    sql = """WITH t AS ( SELECT MIN(run) AS run
                         FROM reco_locked
                         WHERE locked = 0
                         AND run > NVL( ( SELECT MAX(run)
                                          FROM reco_locked
                                          WHERE locked = 1 ), 0 ) )
             SELECT CASE
                      WHEN t.run IS NOT NULL THEN t.run
                      ELSE ( SELECT MAX(run) + 1
                             FROM reco_locked )
                    END
             FROM t
             """

    c, _ = self.api.execute(sql)

    return [ c.fetchall()[0][0] ]
