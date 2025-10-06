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
    try:
        table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
        table.put_item(Item={'SensorID': f"{entity_id}:{property_name}", 'Timestamp': timestamp_iso, 'Valor': value})
        print(f"Sucesso (dataWriter): Histórico salvo no DynamoDB para '{property_name}'.")
    except Exception as e:
        print(f"AVISO (dataWriter): Falha ao salvar no DynamoDB: {e}")
        raise e

def write_to_s3(entity_id, property_name, timestamp_iso, value):
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
    print(f"Evento recebido: {json.dumps(event)}")

    # --- ROTEAMENTO DE EVENTOS ---

    # CAMINHO 1: Chamada do IoT Core (Node-RED) - Identificada pela chave "topic"
    if "topic" in event:
        # ... (código do dataWriter a partir do IoT Core - sem alterações)
        print("Chamada do IoT Core (para iniciar escrita) detectada.")
        try:
            entity_id = os.environ["TWINMAKER_ENTITY_ID"]
            component_name = os.environ["TWINMAKER_COMPONENT_NAME"]
            value, topic = event['value'], event['topic']
            property_name = topic.split('/')[-1]
            timestamp_iso = datetime.now(timezone.utc).isoformat()
            valor_booleano = bool(value)
            
            twinmaker_client.batch_put_property_values(
                workspaceId=WORKSPACE_ID,
                entries=[{'entityPropertyReference': {'entityId': entity_id, 'componentName': component_name, 'propertyName': property_name}, 'propertyValues': [{'value': {'booleanValue': valor_booleano}, 'time': timestamp_iso}]}]
            )
            print(f"Sucesso: Chamada para o TwinMaker enviada para '{property_name}'.")
            return {'statusCode': 200, 'body': json.dumps('Chamada ao TwinMaker enviada com sucesso.')}
        except Exception as e:
            print(f"ERRO no fluxo IoT Core: {e}")
            return {'statusCode': 500, 'body': json.dumps(f"Erro ao chamar o TwinMaker: {e}")}

    # CAMINHO 2: Chamada do TwinMaker para escrever dados (dataWriter) - Identificada pela chave "entries"
    elif "entries" in event:
        # ... (código do dataWriter a partir do TwinMaker - sem alterações)
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
            return {"errorEntries": [{"error": {"code": "INTERNAL_FAILURE", "message": str(e)}, "entryId": "error"}]}
    
    # CAMINHO 3: Chamada do TwinMaker para ler dados (dataReader) - Lógica Universal
    # Identifica uma chamada de leitura pela presença de "selectedProperties" (Grafana) OU "propertyReferences" (Testes)
    elif "selectedProperties" in event or "propertyReferences" in event:
        print("Chamada interna do TwinMaker (dataReader) detectada.")
        try:
            table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
            property_references_to_query = []

            # Normaliza o evento para um formato único
            if "propertyReferences" in event:
                # Formato do nosso evento de teste
                print("Formato de evento 'propertyReferences' detectado.")
                property_references_to_query = event['propertyReferences']
            else:
                # Formato do Grafana/Console TwinMaker
                print("Formato de evento 'selectedProperties' detectado.")
                entity_id = event['entityId']
                component_name = event['componentName']
                for prop_name in event['selectedProperties']:
                    property_references_to_query.append({
                        "entityId": entity_id,
                        "componentName": component_name,
                        "propertyName": prop_name
                    })

            # Agora, faz a query para cada propriedade normalizada
            response_values = []
            for prop_ref in property_references_to_query:
                entity_id = prop_ref['entityId']
                property_name = prop_ref['propertyName']
                sensor_id = f"{entity_id}:{property_name}"
                
                query_response = table.query(
                    KeyConditionExpression=Key('SensorID').eq(sensor_id),
                    ScanIndexForward=False,
                    Limit=50
                )
                
                values_for_property = []
                if query_response['Items']:
                    for item in reversed(query_response['Items']): # Inverte para ordem cronológica
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
            return {"propertyValues": [], "nextToken": None}

    # CAMINHO 4: Se nenhuma condição for satisfeita
    else:
        print("Chamada não reconhecida, retornando resposta vazia válida.")
        return {"propertyValues": [], "nextToken": None}