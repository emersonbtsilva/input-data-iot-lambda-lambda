import boto3
import json
import os
from datetime import datetime, timezone

# --- Clientes AWS ---
twinmaker_client = boto3.client('iottwinmaker')
dynamodb_resource = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# --- Variáveis de Ambiente ---
WORKSPACE_ID = os.environ["TWINMAKER_WORKSPACE_ID"]
DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
TWINMAKER_COMPONENT_NAME = os.environ["TWINMAKER_COMPONENT_NAME"]
ENTITY_ID = os.environ["TWINMAKER_ENTITY_ID"]

def lambda_handler(event, context):
    """
    Lambda inteligente para o Gêmeo Digital.
    - Se for chamada pelo IoT Core, distribui os dados.
    - Se for chamada de volta pelo TwinMaker, apenas confirma o recebimento.
    """
    print(f"Evento recebido: {json.dumps(event)}")

    # --- VERIFICAÇÃO DE ORIGEM ---
    # Se as chaves 'value' e 'topic' não existirem, assumimos que é uma chamada
    # do TwinMaker (o dataWriter). Nesse caso, apenas retornamos um sucesso vazio.
    if 'value' not in event or 'topic' not in event:
        print("Chamada interna do TwinMaker detectada. Encerrando com sucesso.")
        return { "status": "succeeded" } # TwinMaker espera um JSON, não um null.

    # --- FLUXO PRINCIPAL (só executa se for chamada pelo IoT Core) ---

    # 1. Extrair e Validar Dados do Evento
    try:
        value = event['value']
        topic = event['topic']
        property_name = topic.split('/')[-1]
        
        now_utc = datetime.now(timezone.utc)
        timestamp_iso = now_utc.isoformat()
        valor_booleano = bool(value)

    except (KeyError, IndexError) as e:
        print(f"ERRO CRÍTICO: Evento de entrada inválido: {e}")
        return { "status": "failed", "error": str(e) }

    # 2. Atualiza o estado no AWS IoT TwinMaker (Para o Grafana)
    try:
        property_entry = {
            'entityPropertyReference': {
                'entityId': ENTITY_ID,
                'componentName': TWINMAKER_COMPONENT_NAME,
                'propertyName': property_name
            },
            'propertyValues': [{
                'value': {'booleanValue': valor_booleano},
                'time': now_utc.isoformat()
            }]
        }
        twinmaker_client.batch_put_property_values(
            workspaceId=WORKSPACE_ID,
            entries=[property_entry]
        )
        print(f"Sucesso: Chamada para o TwinMaker enviada para '{property_name}'.")
    except Exception as e:
        print(f"AVISO: Falha ao chamar o TwinMaker: {e}")

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
        s3_key = f"dados-atuadores/{now_utc.year}/{now_utc.month}/{now_utc.day}/{property_name}-{int(now_utc.timestamp())}.json"
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

    return {'statusCode': 200, 'body': json.dumps('Processamento principal concluído.')}