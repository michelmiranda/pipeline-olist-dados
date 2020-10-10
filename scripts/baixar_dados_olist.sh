#!/bin/bash
echo 'Fazendo download de dados'
set -e
git clone https://github.com/olist/work-at-olist-data.git
mv work-at-olist-data/datasets/ .
echo 'Transferindo dados'
rm -fr  work-at-olist-data
echo 'Removendo arquivos.'