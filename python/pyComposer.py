__app_name__ = "ComposerCLI"
__version__ = "0.1.0"

import argparse
import google.auth
import requests
import subprocess

from google.auth.transport.requests import AuthorizedSession
from typing import Any


def make_composer2_web_server_request(
    credentials: str, url: str, method: str = "GET", **kwargs: Any
) -> google.auth.transport.Response:
    """
    Make a request to Cloud Composer 2 environment's web server.
    Args:
      url: The URL to fetch.
      method: The request method to use ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT',
        'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                  If no timeout is provided, it is set to 90 by default.
    """

    authed_session = AuthorizedSession(credentials)

    # Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(method, url, **kwargs)


def trigger_dag(credentials: str, web_server_url: str, dag_id: str, data: dict) -> str:
    """
    Make a request to trigger a dag using the stable Airflow 2 REST API.
    Args:
      web_server_url: The URL of the Airflow 2 web server.
      dag_id: The DAG ID.
      data: Additional configuration parameters for the DAG run (json).
    """

    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{web_server_url}/{endpoint}"
    json_data = {"conf": data}

    response = make_composer2_web_server_request(
        credentials, request_url, method="POST", json=json_data
    )

    if response.status_code == 403:
        raise requests.HTTPError(
            "You do not have a permission to perform this operation. "
            "Check Airflow RBAC roles for your account."
            f"{response.headers} / {response.text}"
        )
    elif response.status_code != 200:
        response.raise_for_status()
    else:
        return response.text


def main():
    parser = argparse.ArgumentParser(
        "composer",
        "CLI for Cloud Composer",
        epilog="",
        add_help=True,
    )
    parser.add_argument("--env", help="Environment name")
    parser.add_argument("--location", help="Environment location")
    parser.add_argument("--dag_id", help="DAG ID")
    args = parser.parse_args()

    AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
    while True:
        try:
            CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])
            print("Authentication complete \n")
        except Exception as e:
            print(e, "Validating authentication... \n")
            subprocess.run(
                "gcloud auth application-default login", shell=True, check=True
            )
            CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])
        else:
            break

    if CREDENTIALS is not None:
        dag_id = args.dag_id
        dag_config = {}
        url_output = subprocess.run(
            f'gcloud composer environments describe {args.env} --location={args.location} --format="value(config.airflowUri)"',
            shell=True,
            check=True,
            capture_output=True,
        )
        web_server_url = url_output.stdout.decode("utf-8").strip("\r\n")

        response_text = trigger_dag(
            credentials=CREDENTIALS,
            web_server_url=web_server_url,
            dag_id=dag_id,
            data=dag_config,
        )

        print(response_text)
    else:
        print("Authentication failed.")


if __name__ == "__main__":
    main()
