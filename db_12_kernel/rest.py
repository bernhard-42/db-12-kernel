import json
import time

import requests


class RemoteCommand:
    def __init__(
        self, host, cluster, org=None, token=None, log=None, print_function=None,
    ):
        self.cluster = cluster
        self.org = org
        self.log = log

        self.print = print_function

        self.context_id = None

        # Databricks base url
        self.base_url = "%s/api/1.2/" % host.strip("/")

        self.headers = {"Authorization": "Bearer %s" % token}

        if org is not None:
            self.headers["X-Databricks-Org-Id"] = org

    def get(self, url, query_string=None):
        if query_string is not None:
            url = url + "?" + query_string
        response = requests.get(url, headers=self.headers)
        if response.status_code in [200, 201]:
            return response.json()
        else:
            return {"error": response.status_code, "cause": response.text}

    def post(self, url, data):
        response = requests.post(url, headers=self.headers, data=data)
        if response.status_code in [200, 201]:
            return response.json()
        else:
            return {"error": response.status_code, "cause": response.text}

    def start_context(self):
        url = self.base_url + "contexts/create"

        data = {"language": "python", "clusterId": self.cluster}

        response = self.post(url, data=data)
        if response.get("error", None) is None:
            self.context_id = response["id"]
            self.log.info("Context for %s started: %s" % ("python", self.context_id))
            return True
        else:
            self.context_id = None
            print("Error", response)
            return False

    def close_context(self):
        url = self.base_url + "contexts/destroy"
        data = {"contextId": self.context_id, "clusterId": self.cluster}

        response = self.post(url, data=data)
        return response

    def get_status(self, command_id):
        url = self.base_url + "commands/status"

        query_string = "clusterId=%s&contextId=%s&commandId=%s" % (
            self.cluster,
            self.context_id,
            command_id,
        )

        response = self.get(url, query_string)
        return response

    def get_result(self, command_id):
        result = self.get_status(command_id)
        count = 0
        while result["status"] in ["Queued", "Running"]:
            if result["status"] == "Queued":
                self.print(".", end="", flush=True)
            else:
                self.print(".", end="", flush=True)
            time.sleep(0.5)
            count += 1
            result = self.get_status(command_id)
        self.print("\r" + " " * count)

        if result.get("error", None) is None:
            if result["results"]["resultType"] == "error":
                return (
                    False,
                    {"summary": result["results"]["summary"], "cause": result["results"]["cause"],},
                )
            else:
                return (True, result["results"]["data"])
        else:
            return (False, {"summary": "Generic Error", "cause": result})

    def execute(self, cmd):
        url = self.base_url + "commands/execute"
        data = {
            "language": "python",
            "clusterId": self.cluster,
            "contextId": self.context_id,
            "command": cmd,
        }

        response = requests.post(url, headers=self.headers, data=data)
        command_id = response.json()["id"]
        return self.get_result(command_id)
