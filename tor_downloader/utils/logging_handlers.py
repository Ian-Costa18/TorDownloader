"""Logging handler and formatter to use with TQDM.

Add the logging handler to a logger to write logged data to TQDM output.
Add the formatter to format the logs to: "(%(asctime)s) [+/-/?/*/***] %(message)s"

Created by Ian Costa (ian.costa@ankura.com) in July 2022.
"""

import logging

import tqdm
from colorama import Back, Fore, Style, init


class TqdmLoggingHandler(logging.Handler):
    """Logging Handler for TQDM.
    From https://stackoverflow.com/questions/38543506/change-logging-print-function-to-tqdm-write-so-logging-doesnt-interfere-wit"""
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        # try:
        msg = self.format(record)
        tqdm.tqdm.write(msg)
        self.flush()
        # except Exception:
        #     self.handleError(record)

class TDFormatter(logging.Formatter):
    """Logging colored formatter, adapted from https://stackoverflow.com/a/56944256/3638629
    From https://alexandra-zaharia.github.io/posts/make-your-own-custom-color-formatter-with-python-logging/"""
    grey = Style.DIM + Fore.BLACK
    green = Fore.GREEN
    yellow = Fore.YELLOW
    red = Fore.RED
    bold_red = Style.BRIGHT + Fore.RED
    white = Fore.WHITE
    reset = Style.RESET_ALL

    def __init__(self, fmt: str=""):
        super().__init__()
        init()
        self.date_time_fmt = "%m-%d-%y %H:%M"
        self.time_fmt = f"{self.grey}(%(asctime)s){self.reset}"
        self.fmt = fmt or f"{self.time_fmt} %(levelname)s %(message)s"


        self.FORMATS = {
            logging.DEBUG: self.grey,
            logging.INFO: self.green,
            logging.WARN: self.yellow,
            logging.ERROR: self.red + Back.YELLOW,
            logging.CRITICAL: self.bold_red + Back.YELLOW
        }

    def format(self, record: logging.LogRecord) -> str:
        """Formats a record.

        Format looks like this:
            DEBUG:    "%TIME [?] %MSG"
            INFO:     "%TIME [+] %MSG"
            WARN:     "%TIME [-] %MSG"
            ERROR:    "%TIME [*] %MSG"
            CRITICAL: "%TIME [***] %MSG"
        """
        level_fmt = ""
        level_color = self.FORMATS.get(record.levelno)
        match record.levelno:
            case logging.DEBUG:
                level_fmt = level_color + "[?]" + self.reset
            case logging.INFO:
                level_fmt = level_color + "[+]" + self.reset
            case logging.WARN:
                level_fmt = level_color + "[-]" + self.reset
            case logging.ERROR:
                level_fmt = level_color + "[*]" + self.reset
            case logging.CRITICAL:
                level_fmt = level_color + "[***]" + self.reset
            case _:
                raise ValueError(f"Unknown log level: {record.levelno}")

        log_format = self.fmt.replace("%(levelname)s", level_fmt)


        formatter = logging.Formatter(log_format, self.date_time_fmt)
        return formatter.format(record)



    # Right align the time
    # Get the length of the message
    # msg_without_fmt = re.sub("%.", "", record.msg)
    # msg_len = len(msg_without_fmt)
    # for arg in record.args:
    #     msg_len += len(str(arg))
    # # Format the time with the right alignment
    # time_place = log_format.find("%(asctime)s")
    # # log_format = f"{log_format[:time_place-1] : <{msg_len}} {log_format[time_place-1:] : <25}"
    # log_format = f"{log_format[:time_place-1]}" + f"{log_format[time_place-1:]}".ralign()
