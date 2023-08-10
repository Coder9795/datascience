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


existing_vehicles = get_existing_cfy_list()
obj_config = Config()

# declare constants
APP_NAME = "Car For You Scrapper"
URL = 'https://www.carforyou.ch/en/auto/search?page={page}&sortType=PRICE&sortOrder=DESC&mileageTo=100000&firstRegistrationYearFrom=2016'

headers = { 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Content-Type': 'application/json', 'Accept-Encoding': 'gzip, deflate', 'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8', 'Connection': 'keep-alive', 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
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
            title = soup.find("h1", {"class": "text-grey-dark leading-sm font-bold text-xl md:text-2xl"}).getText()
            dealer = soup.find("div", {"class": "font-bold mb-4 text-grey-dark leading-sm underline"}).getText()
            dealer_address = soup.find("div", {"class": "text-sm font-bold text-teal my-12 flex items-center"}).getText().replace('pin outlined icon', '')
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


def get_all_cars_in_page(url, i):
    """

    :param url:
    :return:
    """
    
    response = requests.get(url, headers = headers)
    if response.status_code == 200:
        #try:
            soup = BeautifulSoup(response.content, "html.parser")
            cars = soup.find_all("div", {"class": "pt-10"})
            for i in range(1, 25):
                slug = cars[i].find_all('a', href=True)[0]['href']
                external_id = slug.split('-')[-1]
                car_url = "https://www.carforyou.ch" + str(slug)
                price = float(cars[i].find("p", {"class": "text-grey-dark leading-sm font-bold w-12/12"}).getText().replace('CHF ','').replace('’','')) if cars[i].find("p", {"class": "text-grey-dark leading-sm font-bold w-12/12"}) else None
                title = cars[i].find("h2", {"class": "text-md leading-xs text-grey-dark font-bold mb-10 break-words"}).getText() if cars[i].find("h2", {"class": "text-md leading-xs text-grey-dark font-bold mb-10 break-words"}) else None
                features = cars[i].find("p", {"class": "text-grey-4 md:text-md leading-xs pb-14"}).getText() if cars[i].find("p", {"class": "text-grey-4 md:text-md leading-xs pb-14"}) else None
                tags = []
                if cars[i].find("div", {"class": "flex flex-wrap text-grey-dark"}):
                    features = cars[i].find("div", {"class": "flex flex-wrap text-grey-dark"}).find_all('span', {'class': 'mx-5'})
                    for feature in features:
                        tags.append(feature.getText())
                dealer_name = cars[i].find("div", {"class": "w-12/12 flex flex-wrap"}).find('div',{'class':'font-bold mb-4 text-grey-dark leading-sm underline'}).getText() if cars[i].find("div", {"class": "w-12/12 flex flex-wrap"}) else None
                dealer_address = cars[i].find("div", {"class": "w-12/12 flex flex-wrap"}).find('div', {'class': 'text-sm font-bold text-teal my-12 flex items-center'}).getText() if cars[i].find("div", {"class": "w-12/12 flex flex-wrap"}) else None
                phone = cars[i].find("div", {"class": "mb-0 mt-10"}).find("div", {"class": "lg:hidden"}).getText() if cars[i].find("div", {"class": "mb-0 mt-10"}) else None
                json_data = {
                    "external_id": external_id,
                    "title": title,
                    "price": price,
                    "features": str(features),
                    "tags": str(tags),
                    "dealer_name": dealer_name,
                    "dealer_address": dealer_address,
                    "url": car_url,
                    "parent_url": url,
                    "phone": phone
                    }
                table_name = "car_for_you"
                flag = "new_entry"
                print(price)
                if len(existing_vehicles) > 0 and existing_vehicles is not None:
                    filtered_cars = list(filter(lambda c: str(c[0]) == str(json_data["external_id"]), existing_vehicles))
                    for cars[i] in filtered_cars:
                        if int(cars[i][1]) != int(json_data["price"]):
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
        #except Exception as e:
         #   beautify(SPACE, "Error", "2 : " + str(e))
          #  print(car_url, url)
    else:
        beautify(SPACE, "Error", "2 : Page not Found\n")


def main():
    """

    :return:
    """
    start_time = datetime.now()
    # Application start display
    start_display(APP_NAME, start_time)
    
    url = URL.format(page=1)
    beautify(SPACE, "URL", ": " + str(url))
    response = requests.get(url, headers = headers)
    soup = BeautifulSoup(response.content, "html.parser")
    total_pages = int(soup.find_all("li", {"class": "page"})[-1].getText())
    beautify(SPACE, "Pages", ": "+str(total_pages))
    # Scrapping Cars
    beautify(SPACE, "Stage", ": Scrapping Items")
    try:
        for j in range(1, total_pages + 1, THREAD_COUNT):
            all_threads = []
            for i in range(j, min(j + THREAD_COUNT, total_pages + 1)):
                calling_url = URL.format(page=i)
                thread = threading.Thread(target = get_all_cars_in_page, args = (calling_url, i))
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
