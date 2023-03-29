"""

Vivelacar information Scrapper

"""

import threading

from bs4 import BeautifulSoup

from competitor_common import *

# declare constants
APP_NAME = "Vivelacar Scrapper"
SOURCE = "vivelacar"
URL = "https://www.vivelacar.com/DE_CH/cars.html?p="
headers = {
    'Content-Type': 'application/json'
    }

cars = []


def get_each_car_data(url, car_json):
    """
    
    :param url:
    :param car_json:
    :return:
    """
    
    # response = call_limited_api(url, custom_headers = headers)
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        product_attributes_div = soup.find("div", attrs = {"class": "product__attribute-content"})
        if not product_attributes_div:
            return
        brand_li = product_attributes_div.find("li", attrs = {"class": "product__attribute-list-item vehicle_brand"})
        model_li = product_attributes_div.find("li", attrs = {"class": "product__attribute-list-item model"})
        all_price_div = soup.find_all("div", attrs = {"class": "product__option-item"})
        model = None
        brand = None
        if brand_li:
            brand = brand_li.find("span", attrs = {"class": "product__attribute-value"}).text.strip(" :")
        if model_li and brand:
            model = model_li.find("span", attrs = {"class": "product__attribute-value"}).text.strip(" :")
            model = trim_model_name(model, brand)
        if not model:
            product_name_div = soup.find("div", attrs = {"class": "pdp_product_name"})
            product_name = product_name_div.text
            if brand in product_name:
                model = product_name.split(brand)[1].strip()
                if model:
                    model = trim_model_name(model, brand)
            else:
                beautify(SPACE, "Error", ": Model not Found\n")
        for eachPriceDiv in all_price_div:
            price = eachPriceDiv.find("span", attrs = {"class": "price"})
            if price:
                price = price.text
            else:
                continue
            price = int("".join(price.split(".")).split(",")[0].strip("€ "))
            distance = eachPriceDiv.find("span", attrs = {"class": "distance"})
            external_id = car_json['productInfo']['productID']
            vin_number = car_json['productInfo']['attributes']['vin_number']
            json_data = {
                "external_id": external_id,
                "source": SOURCE,
                "brand": brand,
                "model": model,
                "vin_number": vin_number,
                "km": int(distance.text.strip(" km")),
                "n_months": 1,
                "currency": "EUR",
                "price": price,
                "url": car_json['productInfo']['productURL']
                }
            if price != '' and price is not None:
                cars.append(json_data)


def get_all_car_details(page_no = 1):
    """
    
    :param page_no:
    :return:
    """
    url = URL + f"{page_no}"
    response = requests.get(url)
    # response = call_limited_api(url, custom_headers = headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        script = soup.find("script", attrs = {"type": "text/javascript", "src": ""})
        if script:
            script_data = script.string
            data = re.search(".* window.digitalData = \((.*)\);\n", script_data, re.M).group(1)
            json_data = json.loads(data)
            if "product" not in json_data:
                return
            all_car_dict = json_data["product"]
            beautify(SPACE, "Page No/Cars", ": {} / {}".format(page_no, len(all_car_dict)))
            all_threads = []
            for eachCar in all_car_dict:
                each_car_url = eachCar['productInfo']['productURL']
                thread = threading.Thread(target = get_each_car_data, args = (each_car_url, eachCar))
                thread.start()
                all_threads.append(thread)
            for thread in all_threads:
                thread.join()
    
    else:
        beautify(SPACE, "Error", ": Page not Found\n")
    get_all_car_details(page_no + 1)


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
    get_all_car_details()
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
