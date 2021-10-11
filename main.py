import datetime
from helper.ddb import DDB


# Buscar registros por um parâmetro
def teste_buscar():
    ddb = DDB(config="conf_table_registries")

    param = {"columnA":"value-to-find"}

    resultado = ddb.search(param)
    print(resultado)


# Buscar determinadas colunas dos registros, por um parâmetro
def teste_buscar_campos():
    ddb = DDB(config="conf_table_registries")

    param = {"columnA":"value-to-find"}
    campos = "#columnA, #columnB, columnCDE" # os Campos-Chave da tabela devem começar com "#"

    resultado = ddb.search(param, campos)
    print(resultado)


# Inserir registro em tabela DDB
def teste_inserir():
    ddb = DDB(config="conf_table_registries")

    ttl = int(datetime.datetime.timestamp(datetime.datetime.now()) + 16 * 24 * 60 * 60) # time to live control to expires items (+16 days remaining)

    columnBVal = "new"
    currentVersion = "v1"

    ddb.insert(dict_obj={"columnA":"new-value-columnA", "columnB": f'keysort_{columnBVal}__{currentVersion}', "bool_columnF":True, "expirationDate":ttl})


# Atualizar registro em tabela DDB
def teste_atualizar():
    ddb = DDB(config="conf_table_registries")

    ttl = int(datetime.datetime.timestamp(datetime.datetime.now()) + 32 * 24 * 60 * 60) # time to live control to expires items (+32 days remaining)

    columnBVal = "mod"
    currentVersion = "v3"

    ddb.update(dict_obj={"columnA":"mod-value-columnA", "columnB": f'keysort_{columnBVal}__{currentVersion}', "bool_columnF":True, "expirationDate":ttl})

