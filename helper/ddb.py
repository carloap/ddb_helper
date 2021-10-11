import boto3
import json
import decimal
from botocore.exceptions import ClientError
import logging

from helper.cfgDDB import *
from helper.MyFileManager import MyFileManager

logger = logging.getLogger()
logger.setLevel(logging.ERROR)

## Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

# formatar o valor float para decimal para o ato de escrita no DDB
def handle_decimal(obj):
    obj = json.dumps(obj, cls=DecimalEncoder)
    return json.loads(obj, parse_float=decimal.Decimal)

class DDB:
    # Construtor: define o nome da tabela, a chave primaria de partição, e a chave de classificação, 
    # ou define a configuração.
    def __init__(self, table_name=None, table_key=None, table_range_key=None, region="us-east-1", config=None):
        if config is not None:
            self.table_name = cfg_ddb_table[config]["cfg_ddb_table"]
            self.table_key = cfg_ddb_table[config]["cfg_table_key"]
            self.table_range_key = cfg_ddb_table[config]["cfg_table_range_key"]
            self.region = cfg_ddb_table[config]["cfg_aws_region"]
        else:
            self.table_name = table_name
            self.table_key = table_key
            self.table_range_key = table_range_key
            self.region = region


    # @deprecated: Efetuar scan na tabela do DynamoDB, para um arquivo de saída
    def scanToFile(self, filterParams=None, filepath='bucket/dump_ddb.raw'):
        ## #########################
        ## Recurso DynamoDB
        dynamodb = boto3.resource('dynamodb', region_name=self.region)
        table = dynamodb.Table(self.table_name)
        table_key = self.table_key
        table_range_key = self.table_range_key
        ## #########################

        # Tentar ler todos os registros do DDB
        try: 
            if filterParams:
                response = table.scan(**filterParams)
            else:
                response = table.scan()

            # Verificar se a chave "Items" do dict existe
            if 'Items' in response:
                items = response['Items']

                # Se foi feito um scan, então continue para paginar...
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
                    MyFileManager.writeFileAsText(filepath, json_item) # Escrevendo itens em arquivo, linha a linha

        except ClientError as e:
            logger.error("ClientError: " + str(e) )


    # Efetuar buscas por meio de filtros, e opcionalmente especificar campos no retorno
    def search(self, params, projectionExpression=None, ddb_limit=2000):
        ## #########################
        ## Recurso DynamoDB
        dynamodb = boto3.resource('dynamodb', region_name=self.region)
        table = dynamodb.Table(self.table_name)
        table_key = self.table_key
        table_range_key = self.table_range_key
        ## #########################

        # Inicializar
        cond_KeyConditionExpression = "" # variável usada SOMENTE para os campos chave
        cond_FilterExpression = "" # variável usada para atributos não chave
        cond_ExpressionAttributeValues = {}
        cond_ExpressionAttributeNames = {}

        list_of_keys={}
        list_of_attr={}

        # condições aplicáveis para determinar o uso de query() ou scan()
        valid_query_conditions = ["="]
        valid_filter_conditions = ["=", "<", "<=", ">", ">=", "begins_with", "contains"]

        list_search_key = list( filter(lambda a: a in (table_key ), params.keys()) )
        # Explicação: Verificar se a variável "params" contém Primary-Key e, eventualmente, o Range-Key, para usar "query" em vez do "scan"
        bool_use_query_key = all(list( map(lambda a: False if 'condition' in params[a] and params[a]['condition'] not in valid_query_conditions else True, list_search_key) ))
        # iterar todos os parâmetros
        for idx, x in enumerate(params.keys()):
            # Regra Capital:
            # RANGE KEY + PRIMARY KEY (com condição de igualdade) = KEY (KeyConditionExpression) -> query()
            # RANGE KEY + PRIMARY KEY (sem condição de igualdade ou misto) = ATTRIBUTE (FilterExpression) -> scan()
            # RANGE KEY sem PRIMARY KEY = ATTRIBUTE (FilterExpression) -> scan()
            if bool_use_query_key and (x in table_key or x in table_range_key) and len(list_search_key)>0:
                list_of_keys[x] = params[x] # é necessário validar os operadores de comparação/condição
            else:
                list_of_attr[x] = params[x]

            try:
                cond_ExpressionAttributeValues[':'+ str(x).replace(".","_")] = params[x]["value"] if type(params[x]) is dict else params[x]
            except KeyError:
                msg_error = "ERRO: O atributo ["+ x +"] não está formatado como um dicionário válido, o campo [value] não foi encontrado! " + str(params[x])
                logger.error("KeyError: " + msg_error )

            # quebra os campos aninhados para estruturar a query do DDB
            if "." in str(x):
                for xx in str(x).split("."):
                    cond_ExpressionAttributeNames['#'+ str(xx)] = xx
            else:
                cond_ExpressionAttributeNames['#'+ str(x)] = x

        # iterar as chaves (Partition Key e Range Key) capturadas (pode ser usado com "query" caso seja utilizado condição de igualdade)
        for idx, key in enumerate(list_of_keys.keys()):
            # o nome e o valor da Partition Key devem ser especificados como uma condição de igualdade
            condition = "=" # condição padrão de igualdade
            if type(list_of_keys[key]) is dict and 'condition' in list_of_keys[key]:
                if list_of_keys[key]['condition'] in valid_filter_conditions:
                    condition = list_of_keys[key]['condition']

            concatConditionExpression = ""
            if(idx < len(list_of_keys.keys())-1):
                concatConditionExpression = " AND "

            if bool_use_query_key and condition in valid_query_conditions:
                cond_KeyConditionExpression += "#"+key + str(condition) +":"+key + concatConditionExpression
            elif condition in ("begins_with", "contains"):
                cond_KeyConditionExpression += str(condition) + "(#"+ str(key).replace(".",".#") +", :"+ str(key).replace(".","_") +")" + concatConditionExpression
            else:
                cond_KeyConditionExpression += "#"+ str(key).replace(".",".#") + str(condition) +":"+ str(key).replace(".","_") + concatConditionExpression

        # iterar os atributos não-chave-primaria capturados (usado com "scan")
        for idx, attr in enumerate(list_of_attr.keys()):
            condition = "=" # condição padrão de igualdade
            if type(list_of_attr[attr]) is dict and 'condition' in list_of_attr[attr]:
                if list_of_attr[attr]['condition'] in valid_filter_conditions:
                    condition = list_of_attr[attr]['condition']

            concatConditionExpression = ""
            if(idx < len(list_of_attr.keys())-1):
                concatConditionExpression = " AND "

            if condition in ("begins_with", "contains"):
                cond_FilterExpression += str(condition) + "(#"+ str(attr).replace(".",".#") +", :"+ str(attr).replace(".","_") +")" + concatConditionExpression
            else:
                # print("AQUIII") # DEBUG
                cond_FilterExpression += "#"+ str(attr).replace(".",".#") + str(condition) +":"+ str(attr).replace(".","_") + concatConditionExpression
                # cond_FilterExpression += "contains(#"+ str(attr).replace(".",".#") + ", " +":"+ str(attr).replace(".","_") + ")" + concatConditionExpression
                
            # cond_FilterExpression += "#"+attr+"=:"+attr

        # corrigir erro cirurgicamente, caso haja algum problema de sintaxe no fim...
        if cond_KeyConditionExpression[-5:] == " AND ":
            cond_KeyConditionExpression = cond_KeyConditionExpression[:-5]
        if cond_FilterExpression[-5:] == " AND ":
            cond_FilterExpression = cond_FilterExpression[:-5]

        # # DEBUG
        # print("FilterExpression:", cond_FilterExpression)
        # print("KeyConditionExpression:", cond_KeyConditionExpression)
        # print("ExpressionAttributeValues:", cond_ExpressionAttributeValues)
        # print("ExpressionAttributeNames:", cond_ExpressionAttributeNames)

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
                    ddb_query_params['ExpressionAttributeNames'][p.replace(' ','')] = p.replace(' ','').replace('#','') # gambs

        # Incluir limite: Note que o "Limit" do DynamoDB não funciona igual ao de um RDBMS. 
        # Esse campo é utilizado para fins de performance. Lembrando que uma query pode ter até 1MB de capacidade computacional.
        # Com "Limit" a query retornará uma quantidade X de resultados, e em seguida aplicará os filtros quando necessário, não ao contrário.
        # @see: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Limit
        if ddb_limit:
            ddb_query_params['Limit'] = ddb_limit

        ddb_args = {**ddb_query_params}

        try: 
            is_ddb_scan = True
            if ddb_query_params:
                if len(cond_KeyConditionExpression)>0:
                    # print("Query")
                    is_ddb_scan = False
                    response = table.query(**ddb_args)
                else:
                    # print("Scan")
                    is_ddb_scan = True
                    response = table.scan(**ddb_args)
            else:
                # print("Full Scan")
                is_ddb_scan = True
                response = table.scan(**ddb_args)

            # Verificar se a chave "Items" do dict existe
            if 'Items' in response:
                items = response['Items']

                # Se foi feito um scan, então continue para paginar...
                # Se o campo 'LastEvaluatedKey' for retornado, então faça outro scan. Ele informa a última chave retornada caso haja mais registros para paginar.
                while 'LastEvaluatedKey' in response:
                    if is_ddb_scan:
                        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **ddb_args)
                    else:
                        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'], **ddb_args)
                    items.extend(response['Items'])

                # print(items)
                json_item = json.dumps(items, cls=DecimalEncoder)
                json_item = json.loads(json_item)
                return json_item

        except ClientError as e:
            logger.error("ClientError: " + str(e))


    # Atualiza um registro incluindo ou sobreescrevendo campos específicos, a partir das chaves
    # @see: https://docs.aws.amazon.com/pt_br/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
    def update(self, dict_obj):
        ## #########################
        ## Recurso DynamoDB
        dynamodb = boto3.resource('dynamodb', region_name=self.region)
        table = dynamodb.Table(self.table_name)
        table_key = self.table_key
        table_range_key = self.table_range_key
        ## #########################

        # Inicializar
        cond_Key = {} # Variável usada SOMENTE para os campos chave
        cond_UpdateExpression = "set " # Variável usada para atributos não chave. Os campos que serão atualizados
        cond_ExpressionAttributeValues = {} # Valores dos respectivos campos

        # Iterar para separar as chaves
        for k in table_key:
            if k in dict_obj:
                cond_Key[k] = dict_obj[k]
                del dict_obj[k]
        for rk in table_range_key:
            if rk in dict_obj:
                cond_Key[rk] = dict_obj[rk]
                del dict_obj[rk]

        # Iterar para obter os campos editáveis
        for idx, v in enumerate(dict_obj):
            concat = ", " if idx < len(dict_obj)-1 else ""
            cond_UpdateExpression += str(v) + "=:" + str(v).replace(".","_") + concat
            cond_ExpressionAttributeValues[":"+ str(v).replace(".","_")] = dict_obj[v]

        # # DEBUG
        # print("Key:", cond_Key)
        # print("UpdateExpression:", cond_UpdateExpression)
        # print("ExpressionAttributeValues:", cond_ExpressionAttributeValues)

        # Preenche o dicionario com os parâmetros utilizados
        ddb_query_params = {"ReturnValues": "UPDATED_NEW"}
        ddb_query_params['Key'] = cond_Key
        ddb_query_params['UpdateExpression'] = cond_UpdateExpression
        ddb_query_params['ExpressionAttributeValues'] = cond_ExpressionAttributeValues
        # print("redo ExpressionAttrNms: ", ddb_query_params)

        try:
            response = table.update_item(**ddb_query_params)
            return response
        except Exception as e:
            logger.error(str(e))
            


    # Inserir objeto
    def insert(self, dict_obj):
        ## #########################
        ## Recurso DynamoDB
        dynamodb = boto3.resource('dynamodb', region_name=self.region)
        table = dynamodb.Table(self.table_name)
        table_key = self.table_key
        table_range_key = self.table_range_key
        ## #########################

        dict_obj = handle_decimal(dict_obj)

        try:
            # Tentar inserir um registro novo, ou sobreescrever um já existente com base nas chaves
            response = table.put_item(Item=dict_obj)
            return response
        except ClientError as e:
            logger.error(str(e))


    # Efetuar uma exclusão de um registro do DynamoDB
    def delete(self, key_obj):
        ## #########################
        ## Recurso DynamoDB
        dynamodb = boto3.resource('dynamodb', region_name=self.region)
        table = dynamodb.Table(self.table_name)
        table_key = self.table_key
        table_range_key = self.table_range_key
        ## #########################

        try:
            # Tenta remover um registro com base nas chaves
            response = table.delete_item(Key=key_obj)
            return response
        except ClientError as e:
            logger.error(str(e))


##########
## Try it:
# python3 -c 'import ddb; ddb.scanToFile({"ProjectionExpression":"#keyA,#keyRange", "ExpressionAttributeNames":{"#keyA":"keyA","#keyRange":"keyRange"}});'
# python3 -c 'import ddb; ddb.search({"columnA": {"value":"value-equals","condition":"="}, "columnB": "value-of-column-b"}, None)'
