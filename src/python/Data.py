from WMCore.REST.Server import DatabaseRESTApi
from WMCore.REST.Format import RawFormat
from T0WmaDataSvc.DataHello import *
from T0WmaDataSvc.DataRecoConfig import *
from T0WmaDataSvc.DataRunConfig import *
from T0WmaDataSvc.DataExpressConfig import *
from T0WmaDataSvc.DataFirstConditionSafeRun import *
from T0WmaDataSvc.DataRunStreamDone import *
from T0WmaDataSvc.DataRunDatasetDone import *
from T0WmaDataSvc.DataPromptRecoStatus import *
from T0WmaDataSvc.DataDatasetLocked import *
from T0WmaDataSvc.DataRepackStats import *
from T0WmaDataSvc.DataRunStreamSkippedLumis import *
from T0WmaDataSvc.DataPrimaryDatasetConfig import *

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
                "run_config": RunConfig(app, self, config, mount),
                "firstconditionsaferun": FirstConditionSafeRun(app, self, config, mount),
                "run_stream_done": RunStreamDone(app, self, config, mount),
                "run_dataset_done": RunDatasetDone(app,self, config, mount),
                "dataset_locked": DatasetLocked(app, self, config, mount),
                "promptreco_status": PromptRecoStatus(app, self, config, mount),
                "repack_stats": RepackStats(app, self, config, mount),
                "skipped_streamers": RunStreamSkippedLumis(app, self, config, mount),
                "primary_dataset_config": PrimaryDatasetConfig(app, self, config, mount)
                })
