import json
import logging
import boto3
import os
from datetime import datetime, timezone

# Configura logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente do TwinMaker
twinmaker = boto3.client("iottwinmaker")

# Variáveis de ambiente da Lambda
WORKSPACE_ID = os.getenv("WORKSPACE_ID")    # ID do workspace TwinMaker
ENTITY_ID = os.getenv("ENTITY_ID")          # ID da entidade a ser atualizada
COMPONENT_TYPE_ID = os.getenv("COMPONENT_TYPE_ID")  # Tipo do componente
COMPONENT_NAME = os.getenv("COMPONENT_NAME")        # Nome do componente na entidade

# Propriedades que a Lambda vai atualizar
PROPERTIES = ["temperature", "humidity", "status", "timestamp"]

def lambda_handler(event, context):
    """
    Lambda que recebe dados do mock e atualiza múltiplas propriedades no TwinMaker.
    """
    logger.info("Received event: %s", json.dumps(event))

    try:
        # Cria dict de atualizações de propriedades
        property_updates = {}
        for prop in PROPERTIES:
            if prop == "timestamp":
                value = datetime.now(timezone.utc).isoformat()
            else:
                value = event.get(prop)

            if isinstance(value, (int, float)):
                val_dict = {"value": {"doubleValue": value}}
            else:
                val_dict = {"value": {"stringValue": str(value)}}

            property_updates[prop] = val_dict

        # Atualiza a entidade no TwinMaker
        response = twinmaker.update_component(
            workspaceId=WORKSPACE_ID,
            componentTypeId=COMPONENT_TYPE_ID,
            componentName=COMPONENT_NAME,
            propertyUpdates=property_updates
        )

        logger.info("TwinMaker update response: %s", response)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Success", "input": event})
        }

    except Exception as e:
        logger.error("Error updating TwinMaker: %s", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
