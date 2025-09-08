import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    AWS Lambda template handler.

    Args:
        event (dict): Event data passed to the Lambda.
        context (LambdaContext): Runtime information.

    Returns:
        dict: Response object with statusCode, headers, and body.
    """
    logger.info("Received event: %s", json.dumps(event))

    try:
        # TODO: Add your business logic here
        response = {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Success", "input": event})
        }
    except Exception as e:
        logger.error("Error: %s", str(e))
        response = {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }

    return response