#!/bin/bash

# Caminho do diretório onde o script Python está localizado
DIR="$(dirname "$(readlink -f "$0")")"

# Caminho para o script Python
SCRIPT_PATH="$DIR/Criar_Projeto.py"

# Executar o script Python passando o caminho do diretório como argumento
qgis -f "$SCRIPT_PATH" -F "$DIR"
