import boto3
import json
import os
from datetime import datetime, timezone

# --- Clientes AWS ---
twinmaker_client = boto3.client('iottwinmaker')
dynamodb_resource = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# --- Variáveis de Ambiente ---

WORKSPACE_ID = os.environ.get("TWINMAKER_WORKSPACE_ID")
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

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
    """
    try:
        table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
        table.put_item(Item={'SensorID': f"{entity_id}:{property_name}", 'Timestamp': timestamp_iso, 'Valor': value})
        print(f"Sucesso (dataWriter): Histórico salvo no DynamoDB para '{property_name}'.")
    except Exception as e:
        print(f"AVISO (dataWriter): Falha ao salvar no DynamoDB: {e}")

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
    """
    try:
        now_utc = datetime.fromisoformat(timestamp_iso.replace('Z', '+00:00'))
        s3_key = f"dados-atuadores/{now_utc.year}/{now_utc.month}/{now_utc.day}/{property_name}-{int(now_utc.timestamp())}.json"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME, Key=s3_key,
            Body=json.dumps({"sensorId": f"{entity_id}:{property_name}", "timestamp": timestamp_iso, "value": value})
        )
        print(f"Sucesso (dataWriter): Histórico salvo no S3 em '{s3_key}'.")
    except Exception as e:
        print(f"AVISO (dataWriter): Falha ao salvar no S3: {e}")

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
            - Se chamada por outros (ex: dataReader):
                {"propertyValues": list, "nextToken": None}
    """
    print(f"Evento recebido: {json.dumps(event)}")

    # --- CAMINHO 1: A CHAMADA VEM DO TWINMAKER (dataWriter) ---
    # O evento do dataWriter tem uma estrutura específica, como a chave "propertyValues"
    if "propertyValues" in event and event.get('requestType') == 'WRITE':
        print("Chamada interna do TwinMaker (dataWriter) detectada.")
        try:
            # O evento do dataWriter vem como uma lista de propriedades para escrever
            for prop_entry in event['propertyValues']:
                entity_id = prop_entry['entityPropertyReference']['entityId']
                property_name = prop_entry['entityPropertyReference']['propertyName']
                
                # Para cada propriedade, pode haver múltiplos valores (pontos no tempo)
                for value_entry in prop_entry['propertyValues']:
                    valor_booleano = value_entry['value']['booleanValue']
                    timestamp_iso = value_entry['time']
                    
                    # Executa a escrita real nos bancos de dados
                    write_to_dynamo(entity_id, property_name, timestamp_iso, valor_booleano)
                    write_to_s3(entity_id, property_name, timestamp_iso, valor_booleano)
            
            # Retorna o formato de sucesso que o TwinMaker espera
            return {"errorEntries": []}
        except Exception as e:
            print(f"ERRO no fluxo dataWriter: {e}")
            return {"errorEntries": [{"error": {"code": "INTERNAL_FAILURE", "message": str(e)}, "entryId": "error"}]}

    # --- CAMINHO 2: A CHAMADA VEM DO IOT CORE (Node-RED) ---
    # O evento do IoT Core tem a chave "topic" que estamos usando
    elif "topic" in event:
        print("Chamada do IoT Core detectada.")
        try:
            # Pega os dados das variáveis de ambiente para este fluxo
            entity_id = os.environ["TWINMAKER_ENTITY_ID"]
            component_name = os.environ["TWINMAKER_COMPONENT_NAME"]
            
            value, topic = event['value'], event['topic']
            property_name = topic.split('/')[-1]
            timestamp_iso, valor_booleano = datetime.now(timezone.utc).isoformat(), bool(value)
            
            # A única tarefa é chamar o TwinMaker, que vai chamar esta mesma Lambda de volta
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
            
    # --- CAMINHO 3: Outros tipos de chamada (ex: dataReader) ---
    else:
        print("Chamada não reconhecida (provavelmente dataReader), retornando sucesso vazio.")
        # Para o dataReader, a resposta esperada é um pouco diferente
        return {"propertyValues": [], "nextToken": None}