from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *
from operator import itemgetter

class GlobalTagHistory(RESTEntity):
  """REST entity for retrieving a specific primary dataset."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""
    validate_str('express_global_tag', param, safe, RX_EXPRESS_GLOBAL_TAG, optional = True)
    validate_str('prompt_global_tag', param, safe, RX_PROMPT_GLOBAL_TAG, optional = True)
  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=300)
  def get(self, express_global_tag, prompt_global_tag):
    """Retrieve Global tag history

    :arg str era: the acquisition era
    :returns: global tags, minimum run, and maximum run"""

    sql = """
            SELECT express_config.global_tag gt_express, reco_config.global_tag gt_prompt, MAX(run_config.run) AS max_run, MIN(run_config.run) AS min_run
            FROM run_config
            JOIN express_config ON express_config.run = run_config.run
            JOIN reco_config ON reco_config.run = run_config.run
            """
    sql_express = """
            WHERE express_config.global_tag LIKE :gt_express
            """
    sql_ = """
            GROUP BY express_config.global_tag, reco_config.global_tag
            ORDER BY max_run DESC, min_run DESC
            """

    sql_prompt = """
            WHERE reco_config.global_tag LIKE :gt_prompt
            """
    sql_both = """
            WHERE reco_config.global_tag LIKE :gt_prompt AND express_config.global_tag LIKE :gt_express
            """
    
    

    if express_global_tag is not None and prompt_global_tag is not None:
       sq = sql + sql_both + sql_
       c, _ = self.api.execute(sq, gt_express = '%' + str(express_global_tag) + '%', gt_prompt = '%' + str(prompt_global_tag) + '%')
    elif express_global_tag is not None and prompt_global_tag is None:
       sq = sql + sql_express + sql_
       c, _ = self.api.execute(sq, gt_express = '%' + str(express_global_tag) + '%')
    elif express_global_tag is None and prompt_global_tag is not None:
       sq = sql + sql_prompt + sql_
       c, _ = self.api.execute(sq, gt_prompt = '%' + str(prompt_global_tag) + '%')
    else:
       sq = sql + sql_
       c, _ = self.api.execute(sq)

       
    configs = []
    for result in c.fetchall():

        (gt_express, gt_prompt, max_run, min_run) = result

        config = { "express_global_tag" : gt_express,
                   "prompt_global_tag" : gt_prompt,
                   "max_run" : max_run,
                   "min_run" : min_run }
        configs.append(config)

    return configs