import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web del IGP que contiene los últimos sismos
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"
    
    # Realizar la solicitud HTTP a la página web
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la tabla de sismos en el HTML
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla de sismos en la página web'
        }

    # Extraer las filas de la tabla (últimos sismos reportados)
    rows = []
    for row in table.find_all('tr')[1:]:  # Ignorar el encabezado
        cells = row.find_all('td')
        if len(cells) > 0:
            row_data = {
                'id': str(uuid.uuid4()),  # Generar un ID único
                'fecha': cells[0].text.strip(),
                'hora': cells[1].text.strip(),
                'latitud': cells[2].text.strip(),
                'longitud': cells[3].text.strip(),
                'profundidad': cells[4].text.strip(),
                'magnitud': cells[5].text.strip(),
                'zona': cells[6].text.strip()
            }
            rows.append(row_data)

    # Almacenar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrapping')
    
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': {
            'message': 'Datos de los últimos sismos almacenados correctamente',
            'data': rows
        }
    }
