"""

Tutti information Scrapper

"""

import math
import mysql.connector
import re
import threading
from datetime import datetime
import requests
from bs4 import BeautifulSoup

# local imports
from common import *

existing_vehicles = get_existing_tutti_list()


obj_config = Config()


# declare constants
APP_NAME = "Tutti Scrapper"
URL = "https://www.tutti.ch/de/li/{canton}/fahrzeuge?company_ad=false&o={page}&organic=true&ps=10000"
headers = {
    'Content-Type': 'application/json'
    }
THREAD_COUNT = 10


def get_each_car_detail(url, external_id):
    """
    
    :param url:
    :param external_id:
    :return:
    """

    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            title = soup.find("div", {"class": "_9mKtt hM4zw hM4zw"}).getText()
            upload_date = soup.find("div", {"class": "_9mKtt pRm6L"}).getText()
            user_name = soup.find("div", {"class": "IcWA2"}).find('h4').getText()
            address = soup.find("div", {"class": "IcWA2"}).find('div').getText()
            description = soup.find("div", {"class": "MuiBox-root css-1rc7znr"}).getText()
            details_box = soup.find("div", {"class": "MuiBox-root css-1vy58zk"})
            details_containers = details_box.find_all("div", {"class": "MuiBox-root css-gvlojm"})
            phone = soup.find("span", {"class": "syWNr"}).getText() if soup.find("span", {"class": "syWNr"}) else None
            zip, city, price, odometer_max, odometer_min, reg_year, brand, model, vehicle_type, fuel_type, transmission, color = None, None, None, None, None, None, None, None, None, None, None, None
            for container in details_containers:
                for sub_container in container.find_all("div", {"class": "MuiBox-root css-xdf4mo"}):
                    field = sub_container.find("dt").getText()
                    if field == "Preis CHF":
                        price = int(re.search("(.*).-", sub_container.find("dd").getText()).group(1).replace("'", ""))
                    if field == "PLZ":
                        zip = sub_container.find("dd").getText()
                    if field == "Bezirk":
                        city = sub_container.find("dd").getText()
                    if field == "Kilometerstand":
                        if len(sub_container.find("dd").getText().split()) > 1:
                            odometer_min = int(sub_container.find("dd").getText().split()[0].replace("'", ""))
                            odometer_max = int(sub_container.find("dd").getText().split()[2].replace("'", ""))
                        else:
                            odometer_max = int(sub_container.find("dd").getText().split()[0].replace("'", "").replace("+", ""))
                    if field == "Marke":
                        brand = sub_container.find("dd").getText()
                    if field == "Modell":
                        model = sub_container.find("dd").getText()
                    if field == "Erstzulassung":
                        reg_year = int(sub_container.find("dd").getText().split()[0])
                    if field == "Aufbau":
                        vehicle_type = sub_container.find("dd").getText()
                    if field == "Getriebeart":
                        transmission = sub_container.find("dd").getText()
                    if field == "Farbe":
                        color = sub_container.find("dd").getText()
                    if field == "Treibstoff":
                        fuel_type = sub_container.find("dd").getText()
            if reg_year is not None and reg_year >= 2016 and odometer_max <= 100000:
                json_data = {
                    "external_id": external_id,
                    "user_name": user_name,
                    "address": address,
                    "title": title,
                    "zip": zip,
                    "city": city,
                    "price": price,
                    "odometer_max": odometer_max,
                    "odometer_min": odometer_min,
                    "reg_year": reg_year,
                    "brand": brand,
                    "model": model,
                    "vehicle_type": vehicle_type,
                    "fuel": fuel_type,
                    "transmission": transmission,
                    "color": color,
                    "posted_date": upload_date,
                    "description": description,
                    "url": url,
                    "phone": phone
                    }
                table_name = "tutti_ch"
                flag = "new_entry"
                if len(existing_vehicles) > 0 and existing_vehicles is not None:
                    filtered_cars = filter(lambda c: str(c[0]) == str(json_data["external_id"]), existing_vehicles)
                    for car in filtered_cars:
                        
                        if int(car[1]) != int(json_data["price"]):
                            flag = "new_price"
                        else:
                            flag = "same_price"

                if flag == "new_entry":
                    connection = get_connection()
                    cursor = connection.cursor(buffered = True)
                    names = list(json_data)
                    cols = ", ".join(map(escape_name, names))
                    placeholders = ", ".join(['%({})s'.format(name) for name in names])
                    query = 'INSERT INTO {} ({}) VALUES ({})'.format(table_name, cols, placeholders)
                    cursor.execute(query, json_data)
                    connection.commit()
                    cursor.close()
                    connection.close()
                elif flag == "new_price":
                    connection = get_connection()
                    cursor = connection.cursor(buffered = True)
                    query = f'UPDATE `{table_name}` SET `price` = {json_data["price"]} WHERE `external_id` = {json_data["external_id"]}'
                    cursor.execute(query, json_data)
                    connection.commit()
                    cursor.close()
                    connection.close()
                    
    except Exception as e:
        beautify(SPACE, "Error", ": " + str(e))


def get_all_cars_in_page(url):
    """

    :param url:
    :return:
    """
    response = requests.get(url)
    thread_count = 5
    if response.status_code == 200:
        try:
            soup = BeautifulSoup(response.content, "html.parser")
            if soup.find("div", {"class": "+D7kD wFEw6"}):
                total_divs = len(soup.find("div", {"class": "+D7kD wFEw6"}).find_all(attrs = {"data-automation": "ad"}))
                for j in range(0, total_divs, thread_count):
                    all_threads = []
                    for i in range(j, min(j + thread_count, total_divs)):
                        div = soup.find("div", {"class": "+D7kD wFEw6"}).find_all(attrs = {"data-automation": "ad"})[i]
                        slug = div.find('a', {"class": "css-2bbgiq e1hvux6w13"})['href']
                        external_id = slug.split('/')[-1]
                        url = "https://www.tutti.ch" + str(slug)
                        thread = threading.Thread(target = get_each_car_detail, args = (url, external_id))
                        thread.start()
                        all_threads.append(thread)
                    for thread in all_threads:
                        thread.join()
                        
        except Exception as e:
            beautify(SPACE, "Error", ": " + str(e))
    
    else:
        beautify(SPACE, "Error", ": Page not Found\n")


def main():
    """

    :return:
    """
    start_time = datetime.now()
    # Application start display
    start_display(APP_NAME, start_time)
    canton_list = ['aargau', 'appenzell', 'basel', 'bern', 'freiburg', 'genf', 'glarus', 'graubuenden', 'jura', 'luzern', 'neuenburg', 'nid-obwalden', 'schaffhausen', 'schwyz', 'solothurn', 'st-gallen', 'thurgau', 'tessin', 'uri', 'waadt', 'wallis', 'zug', 'zuerich', 'liechtenstein']
    for canton in canton_list:
        url = URL.format(canton=canton, page=1)
        beautify(SPACE, "URL", ": " + str(url))
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        total_pages = int(re.search("– (.*) Inserat", soup.find("div", {"class": "FaTFy"}).getText()).group(1).replace("'", ""))
        total_pages = math.ceil(total_pages/30)
        beautify(SPACE, "Pages", ": "+str(total_pages))
        # Scrapping Cars
        beautify(SPACE, "Stage", ": Scrapping Items")
        try:
            for j in range(1, total_pages + 1, THREAD_COUNT):
                all_threads = []
                for i in range(j, min(j + THREAD_COUNT, total_pages + 1)):
                    calling_url = URL.format(canton=canton, page=i)
                    thread = threading.Thread(target = get_all_cars_in_page, args = (calling_url,))
                    thread.start()
                    all_threads.append(thread)
                for thread in all_threads:
                    thread.join()
            
            beautify(SPACE, "Status", ": Completed Successfully\n")
        except Exception as e:
            beautify(SPACE, "Error", "1 : " + str(e))
    # Application end display
    end_display(start_time)


if __name__ == "__main__":
    main()
