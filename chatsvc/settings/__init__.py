import os
import sys

def reload_settings_module(module):
    """Reload settings module.

    Settings may change following the adjustment of environment
    variables. In this case, the settings module may be reloaded.
    If this is the case the module will already exist in 
    sys.modules and should be reloaded in case environment
    variables have changed that will impact the settings.
    """
    if module in sys.modules:
        reload(sys.modules[module])

if "SERVICE_ENV" in os.environ:
    if os.environ["SERVICE_ENV"] == "localdev":
        reload_settings_module("settings.localdev_settings")
        from settings.localdev_settings import *
    elif os.environ["SERVICE_ENV"] == "integration":
        reload_settings_module("settings.integration_settings")
        from settings.integration_settings import *
    elif os.environ["SERVICE_ENV"] == "staging":
        reload_settings_module("settings.staging_settings")
        from settings.staging_settings import *
    elif os.environ["SERVICE_ENV"] == "prod":
        reload_settings_module("settings.prod_settings")
        from settings.prod_settings import *
    elif os.environ["SERVICE_ENV"] == "test":
        reload_settings_module("settings.test_settings")
        from settings.test_settings import *
    else:
        reload_settings_module("settings.default_settings")
        from settings.default_settings import *
else:
    reload_settings_module("settings.default_settings")
    from settings.default_settings import *

__all__ = []
