import configparser
from os.path import expanduser

from prompt_toolkit.styles import Style
from prompt_toolkit import print_formatted_text, HTML


def bye(status=0):
    """Standard exit function
    
    Args:
        msg (str, optional): Exit message to be printed. Defaults to None.
    """
    if status != 0:
        print_error("\n=> Exiting")
    sys.exit(status)


def _print(color, *args):
    style = Style.from_dict({"error": "#ff0000", "ok": "#00ff00", "warning": "#ff00ff"})
    print_formatted_text(
        HTML("<{color}>{message}</{color}>".format(color=color, message=escape(" ".join(args)))),
        style=style,
    )


def print_ok(*args):
    _print("ok", *args)


def print_error(*args):
    _print("error", *args)


def print_warning(*args):
    _print("warning", *args)


def get_db_config(profile):
    """Get Databricks configuration from ~/.databricks.cfg for given profile
    
    Args:
        profile (str): Databricks CLI profile string
    
    Returns:
        tuple: The tuple of host and personal access token from ~/.databrickscfg
    """
    config = configparser.ConfigParser()
    configs = config.read(expanduser("~/.databrickscfg"))
    if not configs:
        print_error("Cannot read ~/.databrickscfg")
        bye(1)

    profiles = config.sections()
    if not profile in profiles:
        print(" The profile '%s' is not available in ~/.databrickscfg:" % profile)
        for p in profiles:
            print("- %s" % p)
        bye()
    else:
        host = config[profile]["host"]
        token = config[profile]["token"]
        return host, token
