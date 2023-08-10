#!/usr/bin/env python

"""

Upto Information Scrapper

"""

import threading

import selenium_helper
from competitor_common import *

# declare constants
APP_NAME = "Upto Scrapper"
SOURCE = "upto"
URL = "https://cockpit.upto.ch/frontend-api-v2/cars/getShopItems"
cars = []


def get_token():
    """
    
    :return:
    """
    driver = selenium_helper.init_chrome_driver()
    token = selenium_helper.get_token(driver)
    selenium_helper.close_driver(driver)
    return token


TOKEN = get_token()


def get_each_car_data(car_data):
    """
    
    :param car_data:
    :return:
    """
    external_id = car_data["id"]
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        }
    
    url = f"https://cockpit.upto.ch/frontend-api-v2/cars/{external_id}/getShopItem"
    # response = call_limited_api(url)
    response = requests.get(url, headers = headers)
    if response.status_code == 200:
        json_data = json.loads(response.text)
        charges = json_data["service"]["charges"]
        for charge in charges:
            duration = charge["tariffModifier"]["description"]
            for brand in BRAND_NAMES:
                if brand.lower().split()[0] in car_data["name"].lower():
                    break
            else:
                brand = None
            model = car_data["name"]
            model = trim_model_name(model, brand)
            json_data = {
                "external_id": int(external_id),
                "source": SOURCE,
                "brand": brand,
                "model": model,
                "km": int(duration.split(",")[0].split(":")[1].split("km/Monat")[0]),
                "n_months": int(duration.split(",")[1].split(":")[1].split()[1]),
                "price": charge["amount"],
                "url": "https://cockpit.upto.ch/static/shop.html#/de/product/" + str(external_id)
                }
            if json_data['price'] != '' and json_data['price'] is not None:
                cars.append(json_data)


def get_all_car_details(url):
    """
    
    :param url:
    :return:
    """
    retry_counter = 5
    start = 0
    while start < retry_counter:
        if TOKEN:
            headers = {
                "Authorization": f"Bearer {TOKEN}",
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                }
            response = requests.get(url, headers = headers)
            # response = call_limited_api(url)
            if response.status_code == 200:
                all_car_data = json.loads(response.text)
                all_threads = []
                for car_data in all_car_data:
                    thread = threading.Thread(target = get_each_car_data, args = (car_data,))
                    thread.start()
                    all_threads.append(thread)
                
                for thread in all_threads:
                    thread.join()
            else:
                beautify(SPACE, "Error", ": Page not Found\n")
            break
        start += 1


def main():
    """
    
    :return:
    """
    start_time = datetime.now()
    # Application start display
    start_display(APP_NAME, start_time)
    
    existing_cars_df = get_existing_car_list(SOURCE)
    
    beautify(SPACE, "Existing cars in database", ": " + str((existing_cars_df.shape[0])))
    
    # Scrapping Cars
    beautify(SPACE, "Stage", ": Scrapping Cars\n")
    get_all_car_details(URL)
    beautify(SPACE, "Status", ": Completed Successfully")
    
    beautify(SPACE, "Stage", ": Data transfer to mysql \n")
    beautify(SPACE, "Inserting data to mysql", ": " + str(len(cars)))
    new_cars_df = insert_into_table_competitors(cars, SOURCE)
    beautify(SPACE, "\nStatus", ": Completed Successfully")
    
    removed_car_df = existing_cars_df[~existing_cars_df.apply(tuple, 1).isin(new_cars_df.apply(tuple, 1))]
    
    beautify(SPACE, "Updating removed_car_list in mysql", ": " + str((removed_car_df.shape[0])))
    
    to_update = json.loads(removed_car_df.to_json(orient = 'records'))
    
    update_removed_records(to_update)
    
    # Application end display
    end_display(start_time)


if __name__ == "__main__":
    main()
