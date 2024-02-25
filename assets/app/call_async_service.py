import json
import os
import time

import boto3

from .resume_sfn import SUCCESSFUL_STATUS, TASK_TOKEN_KEY


def handler(event: dict, context: dict) -> dict:
    token = event[TASK_TOKEN_KEY]
    is_manual_callback = event.get("isManualCallback") == "True"
    if not is_manual_callback:
        _emulate_long_running_job(token)
    return {TASK_TOKEN_KEY: token, "isManualCallback": is_manual_callback}


# a dummy function, that emulates calling a service, that invokes a long-running job.
# For example, the `_emulate_long_running_job` triggers a long-running job by making http post request
# and returns immediately.
# The long-running job, once completed, has to publish a message to the sns topic, similar to the
# `_send_callback_message_on_complete` function.
def _emulate_long_running_job(token: str):
    status = _execute_long_running_job()
    _send_callback_message_on_complete(token, status)


def _execute_long_running_job() -> str:
    time.sleep(10)
    return SUCCESSFUL_STATUS


def _send_callback_message_on_complete(token: str, status: str):
    sns_client = boto3.client("sns")

    callback_topic = os.environ["CALLBACK_TOPIC"]
    sns_client.publish(
        TopicArn=callback_topic, Message=json.dumps({TASK_TOKEN_KEY: token, "status": status})
    )
