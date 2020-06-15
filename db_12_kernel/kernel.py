import base64
import logging
import os
import re
import sys
from traitlets import Unicode

from adal import AuthenticationContext
from IPython.display import HTML, display, Image, SVG
import jedi
import jwt
from parso import split_lines
from metakernel import MetaKernel
import requests
import yaml

from db_12_kernel.utils import get_db_config
from db_12_kernel.rest import RemoteCommand
from db_12_kernel.templates import COMPLETION_TEMPLATE, HELP_TEMPLATE, SHOW_TEMPLATE

DB_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"


class DB12Kernel(MetaKernel):
    app_name = "db_12_kernel"
    implementation = "A Jupyter kernel for Databricks using REST API 1.2"
    implementation_version = "0.1.0"
    language = "python"
    language_version = "3.6+"
    banner = "Databricks REST API 1.2 Kernel - evaluates Python statements on a remote Databricks cluster"
    language_info = {
        "mimetype": "text/x-python",
        "name": "python",
        "file_extension": ".py",
        "help_links": MetaKernel.help_links,
    }
    kernel_json = {
        "argv": [sys.executable, "-m", "db_12_kernel", "-f", "{connection_file}"],
        "display_name": "DB Kernel",
        "language": "python",
        "name": "db_12_kernel",
    }

    profile = Unicode(None, allow_none=True).tag(config=True)
    host = Unicode(None, allow_none=True).tag(config=True)
    cluster = Unicode(None, allow_none=True).tag(config=True)
    org = Unicode(None, allow_none=True).tag(config=True)
    tenant = Unicode(None, allow_none=True).tag(config=True)
    user = Unicode(None, allow_none=True).tag(config=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        #        self.log = logging.getLogger(self.__class__.__name__)
        self.log.setLevel(logging.DEBUG)
        self.log.info("Current Databricks config")
        self.log.info("- Profile %s", self.profile)
        self.log.info("- Cluster %s", self.cluster)
        # All clusters
        self.command = None
        self.token = None
        # Azure clusters for User AAD Token
        self.password = None

    #     self._get_config()

    def _get_config(self, config_file="~/.dbjl-light"):
        if self.profile is None:
            with open(os.path.expanduser(config_file)) as fd:
                db_config = yaml.safe_load(fd)
            self.cluster = db_config["cluster"]
            self.host = db_config["host"]
            self.org = db_config["org"]
            self.tenant = db_config["tenant"]
            self.user = db_config["user"]
            self.token = None
        else:
            self.log.info("profile2 %s", self.profile)
            self.host, self.token = get_db_config(self.profile)
            self.log.info("host %s", self.host)
            self.org = None
            self.tenant = None
            self.user = None

        self.host = self.host.rstrip("/")

    def _decode_token(self, token):
        self.log.info(jwt.decode(token, verify=False))

    def _debug(self, *args):
        self.redirect_to_log = True
        self.log.debug(" ".join([str(a) for a in args]) + "\n")
        self.redirect_to_log = False

    def _strip_out(self, result):
        return re.sub(r"^(Out\[\d+\]: )", "", result)

    def _get_password(self, prompt):
        # Using a private API here. No idea how to avoid this ...
        return self._input_request(
            str(prompt), self._parent_ident, self._parent_header, password=True
        )

    def _login(self):
        if self.token is None and self.org is not None:
            # Azure, use User AAD Token
            password = self._get_password("Password for '%s': " % self.user)
            authority_url = "https://login.microsoftonline.com/%s" % self.tenant
            self.auth_context = AuthenticationContext(authority_url)

            token_response = self.auth_context.acquire_token_with_username_password(
                DB_RESOURCE, self.user, password, self.client_id
            )

            self.token = token_response.get("accessToken", None)
            self.refresh_token = token_response.get("refreshToken", None)
            self._decode_token(self.access_token)

    def _create_context(self):
        if self.command == None:
            if self.token is None:
                self._get_config()
            self.log.info("Creating execution context\n")
            self.log.debug("Current config:")
            self.log.debug("- Profile: %s", self.profile)
            self.log.debug("- Databricks host: %s", self.host)
            self.log.debug("- Cluster: %s", self.cluster)
            if self.org is not None:
                self.log.info("- Organsation: %s", self.org)
            if self.user is not None:
                self.log.info("- User: %s\n", self.user)

            self._login()

            self.command = RemoteCommand(
                self.host, self.cluster, self.org, self.token, self.log, self.Print,
            )
            if self.command.start_context():
                self.Print("Importing jedi and pydoc")
                self.command.execute("import jedi, pydoc")
                self.Print("Patching matplotlib.pyplot.show")
                self.command.execute(SHOW_TEMPLATE)
                try:
                    self.Print("Generating Spark UI URL")
                    response = self.command.execute(
                        "sc.getConf().get('spark.databricks.sparkContextId')"
                    )
                    print(response)
                    context_id = self._strip_out(response[1])[1:-1]
                    url = "%ssparkui/%s/driver-%s/jobs?o=%s" % (
                        self.host,
                        self.cluster,
                        context_id,
                        self.org,
                    )
                    url = "%s/?o=%s#/setting/clusters/%s/sparkUi" % (
                        self.host,
                        self.org,
                        self.cluster,
                    )
                    self.command.execute("SPARK_UI='%s'" % url)
                    self.Display(
                        HTML(
                            "Databricks execution context created: <a href='%s'>Spark UI</a> (variable SPARK_UI)"
                            % url
                        ),
                        clear_output=True,
                    )

                except:
                    self.Print("Cannot automatically determine SPARK UI URL")
                    self.Display(HTML("Databricks execution context created"), clear_output=True)
            else:
                self.Display(
                    HTML("Could not start Databricks execution context"), clear_output=False,
                )

    def _close_context(self):
        if self.command is not None:
            self.command.close_context()

    def get_usage(self):
        return "This is a Databricks REST Kernel. It implements a remote Python interpreter."

    def do_execute_direct(self, code):
        self._debug(code)
        self._create_context()

        response = self.command.execute(code)
        if response[0]:
            try:
                result = eval(self._strip_out(response[1]))
            except:
                result = self._strip_out(response[1])

            self._debug("result=", result, type(result))

            if isinstance(result, dict):
                if result.get("client", None) == "__dbjl_light__":
                    if result["format"] in ["png", "jpg", "jpeg"]:
                        self.Display(Image(base64.b64decode(result["data"])))
                        return None
                    elif result["format"] == "html":
                        self.Display(HTML(result["data"]))
                        return None
                    elif result["format"] == "svg":
                        self.Display(SVG(result["data"]))
                        return None

            if result == "":
                return None
            elif isinstance(result, str):
                self.Print(result)
            else:
                return result
        else:
            self.Display(HTML("Summary: " + response[1]["summary"]))
            self.Print(response[1]["cause"])

    def get_completions(self, info):
        self._create_context()

        text = info["code"]
        lines = split_lines(text)
        position = (info["line_num"], info["column"])
        code = COMPLETION_TEMPLATE % (text, str(lines), str(position))

        response = self.command.execute(code)
        result = self._strip_out(response[1])
        before, _, completions = eval(result)
        completions = [before + c for c in completions]
        return [c[info["start"] :] for c in completions]

    def get_kernel_help_on(self, info, level=1, none_on_fail=False):
        last = info["obj"]
        default = None if none_on_fail else ('No help available for "%s"' % last)
        code = HELP_TEMPLATE % (last, default)
        response = self.command.execute(code)
        result = (
            self._strip_out(response[1])
            .replace("``", "`")
            .encode("utf-8")
            .decode("unicode_escape")
            .strip('"')
        )
        self.Print(result)

    def restart_kernel(self):
        self._close_context()

    def do_shutdown(self, restart):
        self._close_context()
        return super().do_shutdown(restart)


if __name__ == "__main__":
    DB12Kernel.run_as_main()
