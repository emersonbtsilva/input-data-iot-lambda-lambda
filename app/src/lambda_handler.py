import json
import logging
import boto3
import os
from datetime import datetime, timezone

# Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente TwinMaker
twinmaker = boto3.client("iottwinmaker")

# Variáveis de ambiente
WORKSPACE_ID = os.getenv("WORKSPACE_ID")        # ID do workspace TwinMaker
ENTITY_ID = os.getenv("ENTITY_ID")              # ID da entidade
COMPONENT_NAME = os.getenv("COMPONENT_NAME")    # Nome do componente na entidade

# Propriedades que a Lambda vai atualizar
PROPERTIES = ["temperature", "humidity", "status", "timestamp"]

def lambda_handler(event, context):
    """
    Recebe dados do mock e atualiza o TwinMaker
    """
    logger.info("Received event: %s", json.dumps(event))

    try:
        # Cria dict de atualizações de propriedades
        property_updates = {}
        for prop in PROPERTIES:
            if prop == "timestamp":
                value = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            elif prop == "status":
                value = event.get(prop) or "OFF"
            else:
                value = event.get(prop)

            if isinstance(value, (int, float)):
                property_updates[prop] = {"doubleValue": value}
            else:
                property_updates[prop] = {"stringValue": str(value)}

        # Atualiza a entidade no TwinMaker
        response = twinmaker.update_entity(
            workspaceId=WORKSPACE_ID,
            entityId=ENTITY_ID,
            componentUpdates={
                COMPONENT_NAME: {
                    "propertyUpdates": property_updates
                }
            }
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
