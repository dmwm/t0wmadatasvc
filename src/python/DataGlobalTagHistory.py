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

    sql_express = """
            SELECT global_tag gt_express, MAX(run) max_run, MIN(run) min_run
            FROM express_config
            WHERE global_tag LIKE :gt_express
            GROUP BY global_tag
            ORDER BY max_run DESC, min_run DESC
            """

    sql_prompt = """
            SELECT global_tag gt_prompt, MAX(run) max_run, MIN(run) min_run
            FROM reco_config
            WHERE global_tag LIKE :gt_prompt
            GROUP BY global_tag
            ORDER BY max_run DESC, min_run DESC
            """
    sql_both = """
            SELECT reco_config.global_tag gt_prompt, express_config.global_tag gt_express, MAX(run_config.run) AS max_run, MIN(run_config.run) AS min_run
            FROM run_config
            JOIN express_config ON express_config.run = run_config.run
            JOIN reco_config ON reco_config.run = run_config.run
            WHERE global_tag LIKE :gt_express AND global_tag LIKE :gt_prompt
            GROUP BY reco_config.global_tag, express_config.global_tag
            ORDER BY max_run DESC, min_run DESC
            """
    sql_none = """
            SELECT reco_config.global_tag gt_prompt, express_config.global_tag gt_express, MAX(run_config.run) AS max_run, MIN(run_config.run) AS min_run
            FROM run_config
            JOIN express_config ON express_config.run = run_config.run
            JOIN reco_config ON reco_config.run = run_config.run
            GROUP BY reco_config.global_tag, express_config.global_tag
            ORDER BY max_run DESC, min_run DESC
            """
    

    if express_global_tag is not None and prompt_global_tag is not None:
       gt_express = '{}%'.format(express_global_tag)
       gt_prompt = '{}%'.format(prompt_global_tag)
       c, _ = self.api.execute(sql_both, gt_express, gt_prompt)
    elif express_global_tag is not None and prompt_global_tag is None:
       gt_express = '{}%'.format(express_global_tag)
       c, _ = self.api.execute(sql_express, gt_express)
    elif express_global_tag is None and prompt_global_tag is not None:
       gt_prompt = '{}%'.format(prompt_global_tag)
       c, _ = self.api.execute(sql_prompt, gt_prompt)
    else:
       c, _ = self.api.execute(sql_none)
       
    configs = []
    for result in c.fetchall():

        (gt_express, gt_prompt, max_run, min_run) = result

        config = { "express_global_tag" : gt_express,
                   "prompt_global_tag" : gt_prompt,
                   "max_run" : max_run,
                   "min_run" : min_run }
        configs.append(config)

    return configs