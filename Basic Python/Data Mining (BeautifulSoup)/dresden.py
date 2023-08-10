# import required libraries

import concurrent.futures
from datetime import datetime
from collections import Counter

import pandas as pd
import requests
from bs4 import BeautifulSoup

# local imports
from common import beautify, end_display, escape_name, get_connection, start_display

# declare constants
URL = "https://www.automobile-dresden.com/fahrzeuge/fahrzeugsuche.html?start={}"
APP_NAME = "Dresden Car Scraper"
TABLE_NAME = "dresden"
counter = 0
SPACE = 39
data = []
car_ids = []


def insert_into_dresden(values):
    """
    Inserts a value into table
    :param values:
    :return:
    """
    connection = get_connection()
    cursor = connection.cursor(buffered = True)
    names = list(values[0])
    cols = ", ".join(map(escape_name, names))
    placeholders = ", ".join(['%({})s'.format(name) for name in names])
    query = 'INSERT INTO {} ({}) VALUES ({}) on duplicate key update price = VALUES(price), net_price = VALUES(net_price)' \
        .format(TABLE_NAME, cols, placeholders)
    cursor.executemany(query, values)
    connection.commit()
    cursor.close()
    connection.close()


def scrapper(url):
    global counter
    counter = counter + 1
    print("Counter - ", counter, " ", url)
    # response = call_scraper_api(url)
    response = requests.get(url)
    try:
        if response.status_code == 200:
            # read contents
            soup = BeautifulSoup(response.content, "html.parser")
            all_div = soup.find("div", {"class": "uk-grid-match automobileteasergrid"})
            for div in all_div.find_all('div', {'class': 'uk-card-body'}):
                name = div.find('h3', {'class': 'uk-card-title automobileteasertitle'}).getText()
                # price = div.find('div', {'class': 'amdd-tagbanner-content-left amdd-tagbanner-content-left-half'}).getText().strip().replace(' EUR', '')
                # image = 'https://www.automobile-dresden.com/' + str(div.find('img', {'class': 'size-auto styled'})['src'])
                url_new = 'https://www.automobile-dresden.com' + str(div.find('a')['href'])
                response_new = requests.get(url_new)
                soup_new = BeautifulSoup(response_new.content, "html.parser")
                info = soup_new.find('dl', {'class': 'atdd-dl'})
                comp_info = pd.DataFrame()
                cleaned_id_text = []
                for i in info.find_all('dt'):
                    cleaned_id_text.append(i.text)
                cleaned_id__attrb_text = []
                for i in info.find_all('dd'):
                    cleaned_id__attrb_text.append(i.text)
                
                comp_info['id'] = cleaned_id_text
                comp_info['attr'] = cleaned_id__attrb_text
                id = soup_new.find('div', {'class': 'kfzdetail_h2_button'}).getText().replace('ID: ', '')
                image = soup_new.find('ul', {'class': 'uk-slideshow-items amdd-slideshow-items'}).find('img')['src']
                price = soup_new.find('p', {'class': 'auto-brutto'}).getText().replace('EUR', '').replace('.', '').strip()
                net_price = soup_new.find('p', {'class': 'auto-netto'}).getText().replace('EUR', '').replace('netto', '').replace('.', '').strip()
                reg_year = comp_info[comp_info['id'] == 'Erstzulassung']['attr'].values[0].strip() if comp_info[comp_info['id'] == 'Erstzulassung']['attr'].values[0].strip() is not None else None
                odometer = comp_info[comp_info['id'] == 'Kilometerstand']['attr'].values[0].replace('km', '').strip() if comp_info[comp_info['id'] == 'Erstzulassung']['attr'].values[0].strip() is not None else None
                performance = comp_info[comp_info['id'] == 'Leistung']['attr'].values[0].strip() if comp_info[comp_info['id'] == 'Erstzulassung']['attr'].values[0].strip() is not None else None
                fuel = comp_info[comp_info['id'] == 'Motor']['attr'].values[0].strip() if comp_info[comp_info['id'] == 'Erstzulassung']['attr'].values[0].strip() is not None else None
                capacity = comp_info[comp_info['id'] == 'Hubraum']['attr'].values[0].replace('ccm', '').strip() if comp_info[comp_info['id'] == 'Erstzulassung']['attr'].values[0].strip() is not None else None
                car_type = comp_info[comp_info['id'] == 'Fahrzeugart']['attr'].values[0].strip() if comp_info[comp_info['id'] == 'Erstzulassung']['attr'].values[0].strip() is not None else None
                transmission = comp_info[comp_info['id'] == 'Getriebe']['attr'].values[0].strip() if comp_info[comp_info['id'] == 'Erstzulassung']['attr'].values[0].strip() is not None else None
                
                json_data = {
                    'id': id,
                    'url': url_new,
                    "name": name,
                    "price": price,
                    "net_price": net_price,
                    "reg_year": reg_year,
                    "odometer": odometer,
                    "performance": performance,
                    "fuel": fuel,
                    "transmission": transmission,
                    "capacity": capacity,
                    "car_type": str(car_type),
                    "image_url": 'https://www.automobile-dresden.com' + str(image)
                    }
                
                data.append(json_data)
                car_ids.append(json_data["id"])
    except Exception as e:
        beautify(SPACE, ": " + str(e))


def get_existing_cars():
    connection = get_connection()
    try:
        cursor = connection.cursor(buffered = True)
        query = 'SELECT id FROM dresden'
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        connection.close()
        return result
    except Exception as e:
        beautify(SPACE, "Error", ": " + str(e))


def main():
    """

    :return:
    """
    start_time = datetime.now()
    # Application start display
    start_display(APP_NAME, start_time)
    
    old_cars = get_existing_cars()
    
    list_of_urls = []
    # for i in range(0, 490, 9):
    for i in range(0, 600, 9):
        url = URL.format(i)
        list_of_urls.append(url)
    
    beautify(SPACE, "Stage", ": Scrapping Cars\n")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers = 20) as executor:
        executor.map(scrapper, list_of_urls)
    
    beautify(SPACE, "Stage", ": Data transfer to mysql \n")
    beautify(SPACE, "Inserting data to mysql", ": " + str(len(data)))
    insert_into_dresden(data)
    
    beautify(SPACE, "Stage", ": checking for cleared cars in mysql \n")
    
    existing_cars = pd.DataFrame(old_cars, columns = ['id'])['id'].tolist()
    
    removed_cars_list = list((Counter(existing_cars) - Counter(car_ids)).elements())

    removed_cars = pd.DataFrame(removed_cars_list, columns = ["id"]).to_json(orient = 'records')
    
    print(removed_cars[:5])
    
    if len(removed_cars_list) > 0:
        beautify(SPACE, "Updating cleared cars in mysql", ": " + str(len(removed_cars_list)))
        connection = get_connection()
        cursor = connection.cursor(buffered = True)
        names = list(removed_cars[0])
        cols = ", ".join(map(escape_name, names))
        placeholders = ", ".join(['%({})s'.format(name) for name in names])
        query = 'INSERT INTO {} ({}) VALUES ({}) on duplicate key update removed_at = CURRENT_TIMESTAMP' \
            .format('dresden', cols, placeholders)
        cursor.executemany(query, removed_cars)
        connection.commit()
        cursor.close()
        connection.close()
    
    # Application end display
    end_display(start_time)


if __name__ == "__main__":
    main()
