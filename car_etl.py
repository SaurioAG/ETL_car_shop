import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "ETL_log.txt"
dataset_file = "transformed_data.csv"

def extract_from_csv(csv_file):
    dataframe = pd.read_csv(csv_file)

    return dataframe

def extract_from_json(json_file):
    dataframe = pd.read_json(json_file, lines=True)

    return dataframe

def extract_from_xml(xml_file):
    dataframe = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    parsed_file = ET.parse(xml_file)
    root = parsed_file.getroot()

    for car in root:
        car_model = car.find('car_model').text
        year = car.find('year_of_manufacture').text
        price = float(car.find('price').text)
        fuel = car.find('fuel').text
        dataframe = pd.concat([dataframe, pd.DataFrame([{'car_model':car_model, 'year_of_manufacture':year, 'price':price, 'fuel':fuel}])], ignore_index=True)
    
    return dataframe

def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

    for csv in glob.glob('*.csv'):
        if "used" in csv:
            extracted_data = pd.concat([extracted_data, extract_from_csv(csv)], ignore_index=True)
    
    for json in glob.glob('*.json'):
        extracted_data = pd.concat([extracted_data, extract_from_json(json)], ignore_index=True)
    
    for xml in glob.glob('*.xml'):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xml)], ignore_index=True)
    
    return extracted_data

def transform(data):
    """
        Converting price from USD to MXN and rounding off to two decimals
    """
    data['price'] = round(data['price']*17,2)
    data.rename(columns = {"price":"price_mxn"}, inplace = True)

    return data

def load_data(transformed_data, file):
    transformed_data.to_csv(file, index = False)

def logging(message, file):
    date_format = '%Y-%h-%d-%H:%M:%S'
    current_date = datetime.now()
    formated_date = current_date.strftime(date_format)
    with open(file, 'a') as log:
        log.write(formated_date + ',' + message + '\n')

#El log agrega timestamps en el mismo archivo cada vez que se corre el script, acumulandolos.
logging('Starting ETL process', log_file)

logging('Extracting data', log_file)
data = extract()
logging('Finished extracting data', log_file)

logging('Transforming data', log_file)
transformed_data = transform(data)
logging('Finished transforming data', log_file)

logging('Loading data into CSV file', log_file) #Se puede mejorar haciendo que el archivo excel se cree de nuevo cada vez que se corre el script
load_data(transformed_data, dataset_file)
logging('Finished loading data into CSV file', log_file)

logging('ETL process Finished', log_file)



