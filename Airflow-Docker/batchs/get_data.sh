FILE=/usr/local/airflow/data/microdados_enade_2019.zip

if [ ! -f "$FILE" ]; then
    curl "http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip" -o "/usr/local/airflow/data/microdados_enade_2019.zip"
fi


