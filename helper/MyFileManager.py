import os
import json
from pathlib import Path

class MyFileManager:
    # Conceder permissão de execução a um arquivo
    def make_executable(path):
        mode = os.stat(path).st_mode
        mode |= (mode & 0o444) >> 2
        os.chmod(path, mode)

    # Ler arquivo
    def readFile(filename):
        with open(filename, 'r', encoding='utf8') as file:
            return file.read()

    # Escreve arquivo, linha a linha
    def writeFile(filename, dict_content):
        # Cria a arvore de diretórios
        filesys_path = Path('/'.join(filename.split('/')[0:-1]))
        filesys_path.mkdir(parents=True, exist_ok=True)
        with open(filename, 'a+', encoding='utf8') as out_file:
            out_file.write(dict_content + '\n') # break line
        # os.chmod(filename, os.stat(filename).st_mode | stat.S_IEXEC)
        MyFileManager.make_executable(filename)

    # Escreve um arquivo como texto, linha a linha
    def writeFileAsText(filename, dict_content):
        # Cria a arvore de diretórios
        filesys_path = Path('/'.join(filename.split('/')[0:-1]))
        filesys_path.mkdir(parents=True, exist_ok=True)
        with open(filename, 'a', encoding='utf8') as out_file:
            json.dump(dict_content, out_file, ensure_ascii=False)
            out_file.write(os.linesep)

    # Deletar um arquivo, se existir
    def removeFile(filename):
        if os.path.exists(filename):
            os.remove(filename)