import boto3
import json
import os
import time 
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# --- Clientes e Variáveis de Ambiente ---
# Inicialização dos serviços da AWS que serão utilizados.
# As variáveis de ambiente são configuradas no próprio console da Lambda.
twinmaker_client = boto3.client('iottwinmaker')
dynamodb_resource = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

WORKSPACE_ID = os.environ.get("TWINMAKER_WORKSPACE_ID")
DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

# --- FUNÇÕES DE ESCRITA ---

def write_to_dynamo(entity_id, property_name, timestamp_iso, value):
    """Persiste o estado de um sensor no DynamoDB.
    
    Esta versão simplificada tenta escrever no DynamoDB uma única vez.
    Se um erro ocorrer, ele será registrado e a exceção será levantada
    para que o TwinMaker possa registrar a falha.

    Args:
        entity_id (str): O ID da entidade no TwinMaker (ex: 'festoEntity').
        property_name (str): O nome da propriedade (ex: 'Avancado_1S2').
        timestamp_iso (str): O timestamp do evento em formato ISO 8601.
        value (bool): O valor booleano do sensor.

    Raises:
        ClientError: Levanta a exceção se a escrita falhar.
    """
    table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
    item_to_save = {'SensorID': f"{entity_id}:{property_name}", 'Timestamp': timestamp_iso, 'Valor': value}
    
    try:
        table.put_item(Item=item_to_save)
        print(f"Sucesso (dataWriter): Histórico salvo no DynamoDB para '{property_name}'.")
    except ClientError as e:
        print(f"ERRO (dataWriter): Falha ao salvar no DynamoDB para '{property_name}': {e}")
        raise e # Desiste e levanta o erro para o TwinMaker.


def write_to_s3(entity_id, property_name, timestamp_iso, value):
    """Salva uma cópia do evento no S3 para arquivamento de longo prazo (Data Lake).
    
    Cada estado do sensor é salvo como um arquivo JSON individual, particionado
    por ano/mês/dia para facilitar futuras análises com serviços como o Athena.
    """
    try:
        ts_str = timestamp_iso.replace('Z', '+00:00')
        now_utc = datetime.fromisoformat(ts_str)
        # Estrutura de pastas para o Data Lake: ano/mes/dia/arquivo.json
        s3_key = f"dados-atuadores/{now_utc.year}/{now_utc.month}/{now_utc.day}/{property_name}-{int(now_utc.timestamp())}.json"
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME, 
            Key=s3_key,
            Body=json.dumps({"sensorId": f"{entity_id}:{property_name}", "timestamp": timestamp_iso, "value": value})
        )
        print(f"Sucesso (dataWriter): Histórico salvo no S3 em '{s3_key}'.")
    except Exception as e:
        print(f"AVISO (dataWriter): Falha ao salvar no S3: {e}")
        raise e

# --- ROTEADOR PRINCIPAL DA LAMBDA ---
def lambda_handler(event, context):
    """Ponto de entrada da função Lambda.
    
    Atua como um roteador, identificando a origem da chamada (IoT Core,
    TwinMaker para escrita, ou TwinMaker para leitura) e direcionando

    para o fluxo de código apropriado.
    """
    print(f"Evento recebido: {json.dumps(event)}")
    
    # --- CAMINHO 1: O SENSOR FALA ---
    # Acionado pela regra do IoT Core quando o Node-RED publica um estado.
    if "topic" in event and "payload" in event:
        print("Chamada do IoT Core (para iniciar escrita) detectada.")
        try:
            entity_id = os.environ["TWINMAKER_ENTITY_ID"]
            component_name = os.environ["TWINMAKER_COMPONENT_NAME"]
            
            value = event['payload']['value']
            topic = event['topic']
            property_name = topic.split('/')[-1] # Extrai o nome do sensor do tópico MQTT
            
            timestamp_iso = datetime.now(timezone.utc).isoformat()
            valor_booleano = bool(value)
            
            # Delega a escrita para o TwinMaker, que chamará esta Lambda de volta.
            twinmaker_client.batch_put_property_values(
                workspaceId=WORKSPACE_ID,
                entries=[{'entityPropertyReference': {'entityId': entity_id, 'componentName': component_name, 'propertyName': property_name}, 'propertyValues': [{'value': {'booleanValue': valor_booleano}, 'time': timestamp_iso}]}]
            )
            print(f"Sucesso: Chamada para o TwinMaker enviada para '{property_name}'.")
            return {'statusCode': 200, 'body': json.dumps('Chamada ao TwinMaker enviada com sucesso.')}
        except Exception as e:
            print(f"ERRO no fluxo IoT Core: {e}")
            return {'statusCode': 500, 'body': json.dumps(f"Erro ao chamar o TwinMaker: {e}")}

    # --- CAMINHO 2: O TWINMAKER ORDENA A ESCRITA ---
    # Acionado pelo TwinMaker após o Caminho 1. É aqui que os dados são salvos.
    elif "entries" in event:
        print("Chamada interna do TwinMaker (dataWriter) detectada.")
        try:
            for entry in event['entries']:
                entity_id = entry['entityPropertyReference']['entityId']
                property_name = entry['entityPropertyReference']['propertyName']
                value_entry = entry['propertyValues'][0]
                valor_booleano = value_entry['value']['booleanValue']
                timestamp_iso = value_entry['time']
                
                # Chama as funções que efetivamente salvam os dados
                write_to_dynamo(entity_id, property_name, timestamp_iso, valor_booleano)
                write_to_s3(entity_id, property_name, timestamp_iso, valor_booleano)
            
            return {"errorEntries": []} # Retorno de sucesso para o TwinMaker
        except Exception as e:
            print(f"ERRO no fluxo dataWriter: {e}")
            return {"errorEntries": [{"error": {"code": "INTERNAL_FAILURE", "message": f"Ocorreu um erro no conector Lambda: {str(e)}"}, "entryId": "error"}]}

    # --- CAMINHO 3: O GRAFANA PERGUNTA ---
    # Acionado pelo TwinMaker quando o Grafana (ou o console) pede o histórico de dados.
    elif "selectedProperties" in event or "propertyReferences" in event:
        print("Chamada interna do TwinMaker (dataReader) detectada.")
        try:
            table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
            property_references_to_query = []

            # Lógica para unificar os dois formatos de evento de leitura (Grafana vs. Teste)
            if "propertyReferences" in event:
                property_references_to_query = event['propertyReferences']
            else: # Formato do Grafana
                entity_id = event['entityId']
                component_name = event['componentName']
                for prop_name in event['selectedProperties']:
                    property_references_to_query.append({
                        "entityId": entity_id,
                        "componentName": component_name,
                        "propertyName": prop_name
                    })
            
            response_values = []
            for prop_ref in property_references_to_query:
                entity_id = prop_ref['entityId']
                property_name = prop_ref['propertyName']
                sensor_id = f"{entity_id}:{property_name}"
                
                # Busca os dados mais recentes no DynamoDB
                query_response = table.query(
                    KeyConditionExpression=Key('SensorID').eq(sensor_id),
                    ScanIndexForward=False, # Ordem decrescente (mais novo primeiro)
                    Limit=100 # Limita a quantidade de pontos por performance
                )
                
                values_for_property = []
                if query_response['Items']:
                    # O Grafana espera os dados em ordem cronológica (mais antigo primeiro)
                    for item in reversed(query_response['Items']):
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
            
    # --- CAMINHO 4: EVENTO DESCONHECIDO ---
    # Se a chamada não corresponder a nenhum dos padrões, retorna uma resposta vazia válida.
    else:
        print("Chamada não reconhecida, retornando resposta vazia válida.")
        return {"propertyValues": [], "nextToken": None}