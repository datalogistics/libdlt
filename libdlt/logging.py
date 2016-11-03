import logging

def setLevel(level):
    getLogger().setLevel(level)
def getLogger():
    class ColourFormatter(logging.Formatter):
        def __init__(self, fmt, datefmt=None):
            self.values = { 
                logging.CRITICAL: {"name": "C", "color": "\033[1;31m"},
                logging.ERROR: {"name": "E", "color": "\033[0;31m"},
                logging.WARNING: {"name": "W", "color": "\033[0;33m"},
                logging.INFO: {"name": "I", "color": "\033[0;32m"},
                logging.DEBUG: {"name": "D", "color": "\033[0;34m"}
            }
            super(ColourFormatter, self).__init__(fmt, datefmt, '{')
        def format(self, record):
            old_fmt = self._style._fmt
            try:
                caller = " {}".format(record.args[0])
            except IndexError:
                caller = ""
            
            record.args = record.args[:-1]
            fmt = old_fmt.format(levelname=self.values[record.levelno]["name"],
                                 color=self.values[record.levelno]["color"],
                                 reset="\033[0m",
                                 caller=caller)
            self._style._fmt = fmt
            result = logging.Formatter.format(self, record)
            self._style._fmt = old_fmt
            return result
    log = logging.getLogger("libdlt")
    if not log.hasHandlers():
        cout = logging.StreamHandler()
        cout.setFormatter(ColourFormatter("{color}[{levelname} {{asctime}}{caller}]{reset} {{message}}"))
        log.addHandler(cout)
    return log

class _log(object):
    op = getLogger().log
    def __init__(self, cls):
        self.cls = cls
        
    def __call__(self, f):
        def wrapper(*args, **kwargs):
            args_str = ""
            kwargs_str = ""
            for arg in args:
                if arg == "":
                    tmpStr = "\"\", "
                else:
                    tmpStr = "{}, ".format(arg)
                    if len(tmpStr) > 100:
                        tmpStr = "..., "
                args_str += tmpStr
            for k, arg in kwargs.items():
                if arg == "":
                    tmpStr = "\"\", "
                else:
                    tmpStr = "{}: {}, ".format(k, arg)
                    if len(tmpStr) > 100:
                        tmpStr = "..., "
                    kwargs_str += tmpStr
            base_str = "{}{}{}".format("args=[{}]" if args_str else "{}", 
                                       ", " if args_str and kwargs_str else "", 
                                       "kwargs={{{}}}" if kwargs_str else "{}")
            base_str = base_str or "No arguments passed{}{}"
            self.op(base_str.format(args_str[:-2], kwargs_str[:-2]), "{}.{}".format(self.cls, f.__name__))
            return f(*args, **kwargs)
            
        return wrapper

class info(_log):
    op = getLogger().info
class debug(_log):
    op = getLogger().debug
class error(_log):
    op = getLogger().error
class critical(_log):
    op = getLogger().critical
class warn(_log):
    op = getLogger().warning
