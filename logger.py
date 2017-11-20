import logging


class Logger:
    def __init__(self):
        self._logger = logging.getLogger('data_verifier')
        self._logger.setLevel(logging.DEBUG)
        # create file handler which logs even debug messages
        debug_fh = logging.FileHandler('debug.log')
        debug_fh.setLevel(logging.DEBUG)
        # create file handler which logs error messages
        error_fh = logging.FileHandler('error.log')
        error_fh.setLevel(logging.ERROR)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s %(threadName)s %(levelname)s %(message)s')
        ch.setFormatter(formatter)
        debug_fh.setFormatter(formatter)
        error_fh.setFormatter(formatter)
        # add the handlers to logger
        self._logger.addHandler(ch)
        self._logger.addHandler(error_fh)
        self._logger.addHandler(debug_fh)

    @property
    def logger(self):
        return self._logger

