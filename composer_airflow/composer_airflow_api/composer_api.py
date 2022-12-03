

"""
Trigger a DAG in Cloud Composer 2 environment using the Airflow 2 stable REST API.
"""

# [START composer_2_trigger_dag]
# [START composer_2_trigger_dag_for_import]
from typing import Any

import google.auth
from google.auth.transport.requests import AuthorizedSession
import requests


# Following GCP best practices, these credentials should be
# constructed at start-up time and used throughout
# https://cloud.google.com/apis/docs/client-libraries-best-practices
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])


def make_composer2_web_server_request(url: str, method: str = "GET", **kwargs: Any) -> google.auth.transport.Response:
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

    authed_session = AuthorizedSession(CREDENTIALS)

    # Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(method, url, **kwargs)


def trigger_dag(web_server_url: str, endpoint: str, method:str="GET", data: dict={}) :
    """
    Make a request to trigger a dag using the stable Airflow 2 REST API.
    https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
    Args:
      web_server_url: The URL of the Airflow 2 web server.
      dag_id: The DAG ID.
      data: Additional configuration parameters for the DAG run (json).
    """

    
    request_url = f"{web_server_url}/{endpoint}"
    json_data = {"conf": data}

    if method == "GET":
        response = make_composer2_web_server_request(
            request_url, method
        )
    else:
        response = make_composer2_web_server_request(
            request_url, method, json=json_data
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
# [END composer_2_trigger_dag_for_import]


if __name__ == "__main__":

    # TODO(developer): replace with your values
    dag_id = "your-dag-id"  # Replace with the ID of the DAG that you want to run.
    dag_config = {
        "your-key": "your-value"
    }  # Replace with configuration parameters for the DAG run.
    # Replace web_server_url with the Airflow web server address. To obtain this
    # URL, run the following command for your environment:
    # gcloud composer environments describe example-environment \
    #  --location=your-composer-region \
    #  --format="value(config.airflowUri)"
    web_server_url = (
        "https://example-airflow-ui-url-dot-us-central1.composer.googleusercontent.com"
    )
    endpoint = f"api/v1/dags/{dag_id}/dagRuns"

    #GET
    response_text = trigger_dag(
        web_server_url=web_server_url, endpoint=endpoint
    )
    print(response_text)


    #POST
    response_text = trigger_dag(
        web_server_url=web_server_url, endpoint=endpoints, method="POST", data=dag_config
    )
    print(response_text)

   