import boto3
import json
import os
from datetime import datetime, timezone

# --- Clientes AWS (inicializados fora para performance) ---
twinmaker_client = boto3.client('iottwinmaker')
dynamodb_resource = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# --- Variáveis de Ambiente (configure na sua Lambda) ---
WORKSPACE_ID = os.environ["TWINMAKER_WORKSPACE_ID"]
DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
TWINMAKER_COMPONENT_NAME = os.environ["TWINMAKER_COMPONENT_NAME"]
ENTITY_ID = os.environ["TWINMAKER_ENTITY_ID"]

def lambda_handler(event, context):
    """
    Lambda focada para o MVP do Gêmeo Digital.
    Recebe dados do IoT Core e distribui para TwinMaker, DynamoDB e S3.
    """
    print(f"Evento recebido: {json.dumps(event)}")

    # 1. Extrair e Validar Dados do Evento
    try:
        value = event['value']
        topic = event['topic']
        property_name = topic.split('/')[-1]
        
        # Gera timestamps uma única vez
        now_utc = datetime.now(timezone.utc)
        timestamp_iso = now_utc.isoformat()
        timestamp_epoch_str = str(int(now_utc.timestamp()))
        valor_booleano = bool(value)

    except (KeyError, IndexError) as e:
        print(f"ERRO CRÍTICO: Evento de entrada inválido: {e}")
        return # Para a execução aqui se o evento for inválido

    # --- Cada bloco abaixo é independente. Uma falha não impede os outros de rodarem. ---

    # 2. Atualiza o estado no AWS IoT TwinMaker (Para o Grafana)
    try:
        twinmaker_client.batch_put_property_values(
            workspaceId=WORKSPACE_ID,
            entries=[{
                'entityId': ENTITY_ID,
                'componentName': TWINMAKER_COMPONENT_NAME,
                'properties': { property_name: { 'value': {'booleanValue': valor_booleano}, 'time': timestamp_epoch_str } }
            }]
        )
        print(f"Sucesso: TwinMaker atualizado para '{property_name}'.")
    except Exception as e:
        print(f"AVISO: Falha ao atualizar o TwinMaker: {e}")

    # 3. Salva o histórico no Amazon DynamoDB
    try:
        table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
        table.put_item(
            Item={
                'SensorID': f"{ENTITY_ID}:{property_name}",
                'Timestamp': timestamp_iso,
                'Valor': valor_booleano
            }
        )
        print(f"Sucesso: Histórico salvo no DynamoDB para '{property_name}'.")
    except Exception as e:
        print(f"AVISO: Falha ao salvar no DynamoDB: {e}")

    # 4. Salva o histórico em um Bucket S3 (Para o Lookout for Equipment)
    try:
        # Particiona os dados por data para facilitar a consulta futura
        s3_key = f"dados-atuadores/{now_utc.year}/{now_utc.month}/{now_utc.day}/{property_name}-{timestamp_epoch_str}.json"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps({
                "sensorId": f"{ENTITY_ID}:{property_name}",
                "timestamp": timestamp_iso,
                "value": valor_booleano
            })
        )
        print(f"Sucesso: Histórico salvo no S3 em '{s3_key}'.")
    except Exception as e:
        print(f"AVISO: Falha ao salvar no S3: {e}")

    return {'statusCode': 200, 'body': json.dumps('Processamento concluído.')}