"""

Flatdrive Information Scrapper

"""

import threading

from competitor_common import *

# declare constants
APP_NAME = "Flatdrive Scrapper"
URL = "https://flatdrive.ch/wp-json/json/export"
conflict_brand = {
    "vw": "Volkswagen",
    "mercedes": "Mercedes-Benz",
    "škoda": "Skoda",
    "new": "Skoda",
    "seat": "Seat",
    "mazda2": "Mazda"
    }
SOURCE = "flatdrive"
km_dict = {
    "xs": 500,
    "s": 900,
    "m": 1600,
    "l": 2100,
    "xl": 3300
    }
month_dict = {
    "48_monate": 48,
    "36_monate": 36,
    "24_monate": 24,
    "12_monate": 12,
    "6_monate": 6,
    "3_monate": 3,
    }
cars = []
counter = 0


def get_car_data(car):
    """
    
    :param car:
    :return:
    """
    
    brand = car["brand"].split()[0]
    if brand.lower() in conflict_brand:
        brand = conflict_brand[brand.lower()]
    model = car["model"]
    model = trim_model_name(model, brand)
    external_id = car["id"]
    url = car["url"]
    global counter
    counter = counter + 1
    # print("Counter - ", counter, " ", external_id)
    for package in car["prices"]:
        if package["price"] != 0:
            json_data = {
                "external_id": int(external_id),
                "source": SOURCE,
                "brand": str(brand),
                "model": str(model),
                "km": int(package["mileage"]),
                "n_months": int(package["duration"] / 30),
                "price": float(package["price"]),
                "url": str(url)
                }
            if json_data["price"] != '' and json_data["price"] is not None:
                cars.append(json_data)


def get_all_car_details(url):
    """
    
    :param url:
    :return:
    """
    # response = call_limited_api(url)
    response = requests.get(url)
    if response.status_code == 200:
        all_car_data = json.loads(response.text)
        # print(all_car_data)
        all_thread = []
        for car in all_car_data:
            thread = threading.Thread(target = get_car_data, args = (car,))
            thread.start()
            all_thread.append(thread)
        for thread in all_thread:
            thread.join()
    else:
        beautify(SPACE, "Error", ": Page not Found")


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
