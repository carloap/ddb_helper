## ##########
## Configurações das tabelas do DDB

cfg_ddb_table = {
    "conf_table_registries": {
        "cfg_aws_region": 'ws-east-3',
        "cfg_ddb_table": 'table_registries', # nome da tabela
        "cfg_table_key": ['columnA'],
        "cfg_table_range_key": ['columnB']
    },
    "conf_table_cities": {
        "cfg_aws_region": 'ws-east-3',
        "cfg_ddb_table": 'table_cities', # nome da tabela
        "cfg_table_key": ['key'],
        "cfg_table_range_key": []
    }
}

# ## ##########