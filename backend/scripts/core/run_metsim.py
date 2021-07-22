from utils.logging import LOG_NAME
from logging import getLogger
import yaml

#----- Setup Logger -----#
log = getLogger(f"{LOG_NAME}.{__name__}")
#------------------------#


class MetSimRunner():
    def __init__(
        self,
        param_path=None
        ) -> None:
        self._param_path = param_path

        self._read_params()

    def _read_params(self):
        log.info("Reading metsim params: %s", self._param_path)
        with open(self._param_path, 'r') as f:
            self.params = yaml.safe_load(f)

        for key in self.params:
            log.info("(%s): %s", key, self.params[key])
