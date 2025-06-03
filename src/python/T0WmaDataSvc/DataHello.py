from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *
from WMCore.REST.Format import JSONFormat, PrettyJSONFormat
from T0WmaDataSvc.Regexps import *

class Hello(RESTEntity):
  """REST entity describing the calling user."""
  def validate(self, apiobj, method, api, param, safe):
    """Validate request input data."""

  @restcall(formats=[('text/plain', PrettyJSONFormat()), ('application/json', JSONFormat())])
  @tools.expires(secs=-1)
  def get(self):
    """Hello world api call.

    ``paramname``
      This is a paramater.

    :returns: world"""

    return "world"
