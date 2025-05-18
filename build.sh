#!/bin/bash

# Iziet no skripta, ja kāda komanda neizdodas
set -e

echo "Uzbūvē Docker attēlus, izmantojot Docker Compose."
# docker-compose build uzbūvē visus servisus, kam definēts 'build' lauks.
# Ja vēlas būvēt tikai jupyterlab attēlu, var izmantot 'docker-compose build jupyterlab'
docker-compose build

echo "Palaiž Docker konteinerus, izmantojot Docker Compose."
# docker-compose up -d palaidīs servisus fonā (detached mode)
# Ja vēlas redzēt darbību žurnālus tieši terminālī, izmanto 'docker-compose up' bez '-d'
docker-compose up -d

echo "Vide tiek startēta. Lūdzu, uzgaidiet, kamēr servisi ir gatavi."
echo "Var pārbaudīt servisu statusu ar komandu: docker-compose ps"
echo "Var apskatīt konteineru žurnalus ar komandu: docker-compose logs"
echo "JupyterLab parasti pieejams: http://localhost:8888"
