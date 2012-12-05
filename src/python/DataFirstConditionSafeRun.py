from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class FirstConditionSafeRun(RESTEntity):
  """REST entity for retrieving an specific run."""

  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data. In this case there is no input data"""

  @restcall
  @tools.expires(secs=300)
  def get(self):
    """Latest run which has not released PromptReco yet

    :returns: Latest run which has not released PromptReco yet'"""

    sql = """WITH reco_locked AS (
              SELECT reco_release_config.run_id AS run_id,
                     CASE
                       WHEN MAX(reco_release_config.released) = 0 AND
                            ( MAX(run.end_time) = 0 OR
                              MAX(run.end_time) + MIN(reco_release_config.delay - reco_release_config.delay_offset) > 0 ) THEN 0
                       ELSE 1
                     END AS locked
              FROM reco_release_config
              INNER JOIN run ON
                run.run_id = reco_release_config.run_id
              GROUP BY reco_release_config.run_id
            )
            SELECT MIN(run_id) AS run_id
            FROM reco_locked
            WHERE locked = 0
            AND run_id > ( SELECT MAX(run_id)
                           FROM reco_locked
                           WHERE locked = 1 )"""

    c, _ = self.api.execute(sql)
    responseRun = c.fetchall()[0][0]
    # Just making it compatible to the previous system
    response = str([{"run_id" : responseRun}])
    
    return response
