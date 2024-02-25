import json
from typing import Optional, Tuple

import boto3

SUCCESSFUL_STATUS = "SUCCEED"
TASK_TOKEN_KEY = "taskToken"


def handler(event: dict, context: dict):
    if "Records" in event:
        assert len(event["Records"]) == 1
        token, is_succeeded, message = extract_job_status_from_sns(event)
    else:
        token, is_succeeded, message = extract_job_status_from_manual_execution(event)

    sfn_client = boto3.client("stepfunctions")
    if is_succeeded:
        sfn_client.send_task_success(taskToken=token, output=message)
    else:
        sfn_client.send_task_failure(taskToken=token, error="Response failed")


def extract_job_status_from_sns(event: dict) -> Tuple[str, Optional[bool], str]:
    record = event["Records"][0]
    payload = json.loads(record["Sns"]["Message"])
    token, status = payload[TASK_TOKEN_KEY], payload["status"]
    is_succeeded = status == SUCCESSFUL_STATUS
    message = record["Sns"]["Message"] if is_succeeded else None
    return token, is_succeeded, message


def extract_job_status_from_manual_execution(event: dict) -> Tuple[str, Optional[bool], str]:
    token, status, message = (
        event[TASK_TOKEN_KEY],
        event.get("status", SUCCESSFUL_STATUS),
        json.dumps(event),
    )
    is_succeeded = status == SUCCESSFUL_STATUS
    message = message if is_succeeded else None
    return token, is_succeeded, message
