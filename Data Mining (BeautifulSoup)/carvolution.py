"""

Carvolution information Scrapper

"""

import threading

from bs4 import BeautifulSoup

# local imports
from competitor_common import *

# declare constants
APP_NAME = "Carvolution Scrapper"
URL = "https://www.carvolution.com/en/cars"
SOURCE = "carvolution"
deductable = 1500
canton = "ZH"  # Zurich code

cars = []
counter = 0

headers = {'Content-Type': 'application/json'}


def get_car_package_info(external_id, km, month, brand, model, car_url):
    """
    Get car package info
    :param external_id:
    :param km:
    :param month:
    :return:
    """
    # get car package info
    url = "https://api.carvolution.com/web/calculation/calculation-request"
    data = {
        "modelId": external_id,
        "subscriptionConfiguration": {
            "kmPerMonth": km,
            "durationInMonths": month,
            "retention": deductable,
            "canton": canton
            },
        "dateOfBirth": None,
        "additionalOptions": []
        }
    data = json.dumps(data)
    response = requests.post(url, data = data, headers = headers)
    
    if response.status_code == 200:
        return_data = json.loads(response.content)
        if return_data is not None:
            price = return_data["subscriptionAmountPerMonth"]
            if price is not None:
                json_data = {
                    "external_id": external_id,
                    "source": SOURCE,
                    "brand": brand,
                    "model": model,
                    "km": km,
                    "n_months": month,
                    "price": price,
                    "url": "https://www.carvolution.com" + car_url
                    }
                if price != '' and price is not None:
                    cars.append(json_data)


def get_each_car_detail(external_id, brand, model, car_url):
    """
    
    :param external_id:
    :param brand:
    :param model:
    :return:
    """
    global counter
    counter = counter + 1
    print("Counter - ", counter, " ", external_id)
    model = trim_model_name(model, brand)
    # get car package info
    all_threads = []
    for km in [850, 1200, 1850, 2500, 3250]:
        for month in [3, 6, 12, 18, 24, 36, 48]:
            thread = threading.Thread(
                target = get_car_package_info, args = (external_id, km, month, brand, model, car_url)
                )
            all_threads.append(thread)
            thread.start()
    for thread in all_threads:
        thread.join()


def get_all_cars_in_page(url):
    """
    
    :param url:
    :return:
    """
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        section = soup.find("section", attrs = {"id": "cars"})
        all_cars_div = section.find_all(
            "div", attrs = {"class": "car-container col-12 col-md-6 col-lg-6 col-xxl-4 ng-star-inserted"}
            )
        for car_div in all_cars_div:
            car_url = car_div.find("a", attrs = {"class": "car ng-star-inserted"})['href']
            car = car_div.find("a", attrs = {"class": "car ng-star-inserted"})
            if car:
                images = car.find_all("img", attrs = {"class": "lazyload ng-star-inserted"})
                external_id = None
                for image in images:
                    img_src = image["data-src"]
                    if "md-" in img_src:
                        external_id = int(img_src.split("md-")[1].split("/")[0])
                        break
                if not external_id:
                    continue
                brand = car_div.find("h3", attrs = {"class": "brand"}).text
                if brand.lower() in CONFLICT_BRAND:
                    brand = CONFLICT_BRAND[brand.lower()]
                model = car_div.find("h2", attrs = {"class": "model"}).text
                get_each_car_detail(external_id, brand, model, car_url)
    
    else:
        beautify(SPACE, "Error", ": Page not Found\n")


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
    try:
        get_all_cars_in_page(URL)
        beautify(SPACE, "Status", ": Completed Successfully")
    except Exception as e:
        beautify(SPACE, "Error", "1 : " + str(e))
    
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
