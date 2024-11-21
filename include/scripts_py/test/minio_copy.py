import boto3
from datetime import timedelta, datetime
import pandas as pd
import random
import string
import json
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_MINIO")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY_MINIO")  # Ajuste conforme necessário para capturar o secret key

# Configuração do bucket no MinIO
BUCKET_NAME = "comercio-importacao-exportacao"  # Certifique-se de que este bucket existe no MinIO
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_MINIO")  # Use a chave de acesso que você criou no MinIO
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY_MINIO")   # Use a chave secreta que você criou no MinIO
MINIO_ENDPOINT = "http://localhost:9000"  # Endpoint do MinIO

# Cliente S3 com boto3 configurado para MinIO
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    endpoint_url=MINIO_ENDPOINT  # Endpoint específico do MinIO
)

# Função para gerar dados aleatórios
def gerar_dados_aleatorios(tipo):
    if tipo == 'nome':
        return ''.join(random.choices(string.ascii_uppercase, k=1)) + ''.join(random.choices(string.ascii_lowercase, k=7))
    elif tipo == 'email':
        return f"{gerar_dados_aleatorios('nome').lower()}@example.com"
    elif tipo == 'data':
        return datetime.now() - timedelta(days=random.randint(0, 365))

# Função para salvar JSON no MinIO
def salvar_json_no_s3(data, bucket, key):
    json_data = json.dumps(data)
    s3_client.put_object(Bucket=bucket, Key=key, Body=json_data)

# Função para gerar e salvar dados no MinIO
def gerar_dados_fonte_3():
    # Fonte 3: Devoluções (JSON)
    devolucoes = []
    for i in range(20000):
        devolucao = {
            'return_id': i + 1,
            'sale_id': random.randint(1, 1000),
            'product_id': random.randint(1, 50),
            'return_date': gerar_dados_aleatorios('data').strftime('%Y-%m-%d'),
            'quantity_returned': random.randint(1, 5),
            'return_reason': random.choice(['Defeito', 'Insatisfação', 'Produto errado'])
        }
        devolucoes.append(devolucao)

    # Salvar JSON no MinIO
    key = "devolucoes_2.json"
    salvar_json_no_s3(devolucoes, BUCKET_NAME, key)

# Executa a função para gerar os dados e salvar no MinIO
gerar_dados_fonte_3()
