import boto3
import json
import os
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key

# --- Clientes e Variáveis de Ambiente ---
twinmaker_client = boto3.client('iottwinmaker')
dynamodb_resource = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

WORKSPACE_ID = os.environ.get("TWINMAKER_WORKSPACE_ID")
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

# --- Funções Auxiliares (sem alterações) ---
def write_to_dynamo(entity_id, property_name, timestamp_iso, value):
    """
    Escreve um registro no DynamoDB.

    Args:
        entity_id (str): ID da entidade do sensor.
        property_name (str): Nome da propriedade do sensor.
        timestamp_iso (str): Timestamp em formato ISO 8601 (UTC).
        value (bool): Valor booleano a ser salvo.

    Returns:
        None

    Raises:
        Exception: Se houver falha ao salvar no DynamoDB.
    """
    try:
        table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
        table.put_item(Item={'SensorID': f"{entity_id}:{property_name}", 'Timestamp': timestamp_iso, 'Valor': value})
        print(f"Sucesso (dataWriter): Histórico salvo no DynamoDB para '{property_name}'.")
    except Exception as e:
        print(f"AVISO (dataWriter): Falha ao salvar no DynamoDB: {e}")
        raise e

def write_to_s3(entity_id, property_name, timestamp_iso, value):
    """
    Escreve um registro no S3 em formato JSON.

    Args:
        entity_id (str): ID da entidade do sensor.
        property_name (str): Nome da propriedade do sensor.
        timestamp_iso (str): Timestamp em formato ISO 8601 (UTC).
        value (bool): Valor booleano a ser salvo.

    Returns:
        None

    Raises:
        Exception: Se houver falha ao salvar no S3.
    """
    try:
        ts_str = timestamp_iso.replace('Z', '+00:00')
        now_utc = datetime.fromisoformat(ts_str)
        s3_key = f"dados-atuadores/{now_utc.year}/{now_utc.month}/{now_utc.day}/{property_name}-{int(now_utc.timestamp())}.json"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME, Key=s3_key,
            Body=json.dumps({"sensorId": f"{entity_id}:{property_name}", "timestamp": timestamp_iso, "value": value})
        )
        print(f"Sucesso (dataWriter): Histórico salvo no S3 em '{s3_key}'.")
    except Exception as e:
        print(f"AVISO (dataWriter): Falha ao salvar no S3: {e}")
        raise e

# --- Função Principal ---
def lambda_handler(event, context):
    """
    Função principal Lambda para integração entre IoT Core, TwinMaker e persistência de dados.

    Args:
        event (dict): Evento recebido pela Lambda. Estrutura varia conforme origem (IoT Core, TwinMaker, etc).
        context (object): Contexto de execução Lambda (não utilizado).

    Returns:
        dict:
            - Se chamada pelo TwinMaker (dataWriter):
                {"errorEntries": list}
            - Se chamada pelo IoT Core:
                {"statusCode": int, "body": str}
            - Se chamada pelo TwinMaker (dataReader):
                {"propertyValues": list, "nextToken": None}
            - Se chamada por outros: dict vazio
    """
    print(f"Evento recebido: {json.dumps(event)}")

    # --- CAMINHO 1: A CHAMADA VEM DO TWINMAKER (dataWriter) ---
    if "entries" in event and "topic" not in event:
        print("Chamada interna do TwinMaker (dataWriter) detectada.")
        try:
            for entry in event['entries']:
                entity_id = entry['entityPropertyReference']['entityId']
                property_name = entry['entityPropertyReference']['propertyName']
                value_entry = entry['propertyValues'][0]
                valor_booleano = value_entry['value']['booleanValue']
                timestamp_iso = value_entry['time']
                write_to_dynamo(entity_id, property_name, timestamp_iso, valor_booleano)
                write_to_s3(entity_id, property_name, timestamp_iso, valor_booleano)
            return {"errorEntries": []}
        except Exception as e:
            print(f"ERRO no fluxo dataWriter: {e}")
            return {"errorEntries": [{"error": {"code": "INTERNAL_FAILURE", "message": f"Ocorreu um erro no conector Lambda: {str(e)}"}, "entryId": "error"}]}

    # --- CAMINHO 2: A CHAMADA VEM DO IOT CORE (Node-RED) ---
    elif "topic" in event:
        print("Chamada do IoT Core detectada.")
        try:
            entity_id = os.environ["TWINMAKER_ENTITY_ID"]
            component_name = os.environ["TWINMAKER_COMPONENT_NAME"]
            value, topic = event['value'], event['topic']
            property_name = topic.split('/')[-1]
            timestamp_iso = datetime.now(timezone.utc).isoformat()
            valor_booleano = bool(value)
            twinmaker_client.batch_put_property_values(
                workspaceId=WORKSPACE_ID,
                entries=[{
                    'entityPropertyReference': {'entityId': entity_id, 'componentName': component_name, 'propertyName': property_name},
                    'propertyValues': [{'value': {'booleanValue': valor_booleano}, 'time': timestamp_iso}]
                }]
            )
            print(f"Sucesso: Chamada para o TwinMaker enviada para '{property_name}'.")
            return {'statusCode': 200, 'body': json.dumps('Chamada ao TwinMaker enviada com sucesso.')}
        except Exception as e:
            print(f"ERRO no fluxo IoT Core: {e}")
            return {'statusCode': 500, 'body': json.dumps(f"Erro ao chamar o TwinMaker: {e}")}

    # --- CAMINHO 3: A CHAMADA VEM DO TWINMAKER (dataReader) ---
    elif event.get('requestType') == 'GET_PROPERTY_VALUE_HISTORY':
        print("Chamada interna do TwinMaker (dataReader) detectada.")
        try:
            table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
            response_values = []
            # O TwinMaker pode pedir o histórico de várias propriedades de uma vez
            for prop_ref in event['propertyReferences']:
                entity_id = prop_ref['entityId']
                property_name = prop_ref['propertyName']
                sensor_id = f"{entity_id}:{property_name}"
                # Faz a query no DynamoDB para pegar o item mais recente
                query_response = table.query(
                    KeyConditionExpression=Key('SensorID').eq(sensor_id),
                    ScanIndexForward=False,  # Ordena do mais novo para o mais antigo
                    Limit=1                  # Pega apenas 1 resultado
                )
                values_for_property = []
                if query_response['Items']:
                    item = query_response['Items'][0]
                    values_for_property.append({
                        "time": item['Timestamp'],
                        "value": {"booleanValue": bool(item['Valor'])}
                    })
                response_values.append({
                    "entityPropertyReference": prop_ref,
                    "values": values_for_property
                })
            response = {"propertyValues": response_values, "nextToken": None}
            print(f"Retornando dados do DynamoDB para o dataReader: {json.dumps(response)}")
            return response
        except Exception as e:
            print(f"ERRO no fluxo dataReader: {e}")
            # Um erro aqui não deve quebrar o TwinMaker, então retornamos uma resposta vazia válida
            return {"propertyValues": [], "nextToken": None}

    # --- CAMINHO 4: Chamada não reconhecida ---
    else:
        print("Chamada não reconhecida, retornando sucesso vazio.")