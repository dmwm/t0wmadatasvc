from WMCore.REST.Server import DatabaseRESTApi
from WMCore.REST.Format import RawFormat
from T0WmaDataSvc.DataHello import *
from T0WmaDataSvc.DataRecoConfig import *
from T0WmaDataSvc.DataExpressConfig import *
from T0WmaDataSvc.DataFirstConditionSafeRun import *
from T0WmaDataSvc.DataRunStreamDone import *
from T0WmaDataSvc.DataDatasetLocked import *

class Data(DatabaseRESTApi):
  """Server object for REST data access API."""
  def __init__(self, app, config, mount):
    """
    :arg app: reference to application object; passed to all entities.
    :arg config: reference to configuration; passed to all entities.
    :arg str mount: API URL mount point; passed to all entities."""

    DatabaseRESTApi.__init__(self, app, config, mount)
    # Makes raw format as default
    self.formats.insert(0, ('application/raw', RawFormat()))
    self._add({ "hello": Hello(app, self, config, mount),
                "express_config": ExpressConfig(app, self, config, mount),
                "reco_config": RecoConfig(app, self, config, mount),
                "firstconditionsaferun": FirstConditionSafeRun(app, self, config, mount),
                "run_stream_done": RunStreamDone(app, self, config, mount),
                "dataset_locked": DatasetLocked(app, self, config, mount)
                })
