import boto3
import json
import os
from datetime import datetime, timezone

# --- Clientes e Variáveis de Ambiente ---
twinmaker_client = boto3.client('iottwinmaker')
dynamodb_resource = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

WORKSPACE_ID = os.environ.get("TWINMAKER_WORKSPACE_ID")
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

# --- Funções Auxiliares (sem alterações) ---
def write_to_dynamo(entity_id, property_name, timestamp_iso, value):
    try:
        table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
        table.put_item(Item={'SensorID': f"{entity_id}:{property_name}", 'Timestamp': timestamp_iso, 'Valor': value})
        print(f"Sucesso (dataWriter): Histórico salvo no DynamoDB para '{property_name}'.")
    except Exception as e:
        print(f"AVISO (dataWriter): Falha ao salvar no DynamoDB: {e}")
        raise e

def write_to_s3(entity_id, property_name, timestamp_iso, value):
    try:
        now_utc = datetime.fromisoformat(timestamp_iso.replace('Z', '+00:00').replace('+00:00',''))
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
    print(f"Evento recebido: {json.dumps(event)}")

    # --- CAMINHO 1: A CHAMADA VEM DO TWINMAKER (dataWriter) ---
    # CORREÇÃO FINAL: A condição agora busca pela chave "entries", que é o que o log real mostrou.
    if "entries" in event and "topic" not in event:
        print("Chamada interna do TwinMaker (dataWriter) detectada.")
        try:
            # O loop agora itera sobre a chave correta: event['entries']
            for entry in event['entries']:
                entity_id = entry['entityPropertyReference']['entityId']
                property_name = entry['entityPropertyReference']['propertyName']
                
                # O valor está dentro de uma lista 'propertyValues' dentro de cada 'entry'
                value_entry = entry['propertyValues'][0]
                valor_booleano = value_entry['value']['booleanValue']
                timestamp_iso = value_entry['time']
                
                # Executa a escrita real nos bancos de dados
                write_to_dynamo(entity_id, property_name, timestamp_iso, valor_booleano)
                write_to_s3(entity_id, property_name, timestamp_iso, valor_booleano)
            
            # Retorna o formato de sucesso que o TwinMaker espera.
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
            
    # --- CAMINHO 3: Outros tipos de chamada (ex: dataReader) ---
    else:
        print("Chamada não reconhecida (provavelmente dataReader), retornando sucesso vazio.")
        return {"propertyValues": [], "nextToken": None}
