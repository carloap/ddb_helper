import os
import json
from pathlib import Path

# Escreve arquivo, linha a linha
def writeFile(filename, dict_content):
    # Cria a arvore de diretórios
    filesys_path = Path('/'.join(filename.split('/')[0:-1]))
    filesys_path.mkdir(parents=True, exist_ok=True)

    with open(filename, 'a', encoding='utf8') as out_file:
        json.dump(dict_content, out_file, ensure_ascii=False)
        out_file.write(os.linesep)
