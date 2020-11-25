import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from cfgDDB import *
import writeFile as wf


## #########################
## DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name=cfg_aws_region)

table = dynamodb.Table(cfg_ddb_table)
table_key = cfg_table_key
table_range_key = cfg_table_range_key
## #########################


## Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)
## #########################


# Efetuar scan na tabela do DynamoDB, para um arquivo de saída
def scanToFile(filterParams=None, filepath='bucket/dump_ddb.raw'):
    # Tentar ler todos os registros do DDB
    try: 
        if filterParams:
            response = table.scan(**filterParams)
        else:
            response = table.scan()

        # Verificar se a chave "Items" existe no dict
        if 'Items' in response:
            items = response['Items']

            # Se o campo 'LastEvaluatedKey' for retornado, então faça outro scan. Ele informa a última chave retornada caso haja mais registros para paginar.
            while 'LastEvaluatedKey' in response:
                if filterParams:
                    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **filterParams)
                else:
                    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response['Items'])

            # Iterar itens
            for item in items:
                json_item = json.dumps(item, cls=DecimalEncoder)
                json_item = json.loads(json_item) # saída pronta
                wf.writeFile(filepath, json_item) # Escrevendo itens em arquivo. TODO: melhoria otimizar I/O

    except ClientError as e:
        print(e.response['Error']['Message'])


def search(filterParams, projectionExpression=None):
    # Inicializar
    cond_KeyConditionExpression = "" # usado somente para as chaves
    cond_FilterExpression = "" # usado para atributos não-chave
    cond_ExpressionAttributeValues = {}
    cond_ExpressionAttributeNames = {}

    list_of_keys={}
    list_of_attr={}

    # iterar parametros
    for idx, x in enumerate(filterParams.keys()):
        if x in table_key or x in table_range_key:
            list_of_keys[x] = filterParams[x]
        else:
            list_of_attr[x] = filterParams[x]
        cond_ExpressionAttributeValues[':'+x] = filterParams[x]
        cond_ExpressionAttributeNames['#'+x] = x

    # separar as chaves
    for idx, key in enumerate(list_of_keys.keys()):
        if(len(list_of_keys.keys())-1 == idx):
            cond_KeyConditionExpression += "#"+key+"=:"+key
        else:
            cond_KeyConditionExpression += "#"+key+"=:"+key + " AND ";

    # separar atributos
    for idx, attr in enumerate(list_of_attr.keys()):
        if(len(list_of_attr.keys())-1 == idx):
            cond_FilterExpression += "#"+attr+"=:"+attr
        else:
            cond_FilterExpression += "#"+attr+"=:"+attr + " AND ";

    # Preenche um dicionário somente com os campos necessários
    ddb_query_params = {}
    if len(cond_KeyConditionExpression)>0:
        ddb_query_params['KeyConditionExpression'] = cond_KeyConditionExpression
    if len(cond_FilterExpression)>0:
        ddb_query_params['FilterExpression'] = cond_FilterExpression
    if len(cond_ExpressionAttributeValues)>0:
        ddb_query_params['ExpressionAttributeValues'] = cond_ExpressionAttributeValues
    if len(cond_ExpressionAttributeNames)>0:
        ddb_query_params['ExpressionAttributeNames'] = cond_ExpressionAttributeNames

    if projectionExpression:
        ddb_query_params['ProjectionExpression'] = projectionExpression
        for p in projectionExpression.split(','):
            if '#' in p and p not in ddb_query_params['ExpressionAttributeNames']:
                ddb_query_params['ExpressionAttributeNames'][p.replace(' ','')] = p.replace(' ','').replace('#','') # gambs para incluir os atributos a serem retornados, caso não esteja explicito em "ExpressionAttributeNames"
    # print("debug ExpressionAttrNms: ", ddb_query_params['ExpressionAttributeNames'])

    try: 
        if ddb_query_params:
            if len(cond_KeyConditionExpression)>0:
                response = table.query(**ddb_query_params)
            else:
                response = table.scan(**ddb_query_params)
        else:
            response = table.scan()

        # Verificar se a chave "Items" existe no dict
        if 'Items' in response:
            items = response['Items']

            if not ddb_query_params:
                # Se o campo 'LastEvaluatedKey' for retornado, então faça outro scan. Ele informa a última chave retornada caso haja mais registros para paginar.
                while 'LastEvaluatedKey' in response:
                    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                    items.extend(response['Items'])
            print(items) # saída...

    except ClientError as e:
        print(e.response['Error']['Message'])


# Inserir objeto, ou sobrescrevê-lo se as chaves já existirem
def insert(dict_obj):
    try:
        # try to insert a single registry
        response = table.put_item(
            Item=dict_obj
        )
        print(response)


# Efetuar uma exclusão de um registro do DynamoDB
def delete(key_params):
    try:
        response = table.delete_item(Key=key_params)
        print(response)

    except ClientError as e:
        print(e.response['Error']['Message'])


## Try it:
# python3 -c 'import ddb; ddb.scanToFile({"ProjectionExpression":"#keyA,#keyRange","ExpressionAttributeNames":{"#keyA":"keyA","#keyRange":"keyRange"}});'