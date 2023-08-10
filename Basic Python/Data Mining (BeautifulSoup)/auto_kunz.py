"""

Auto Kunz Information Scrapper

"""

import threading

from bs4 import BeautifulSoup

from competitor_common import *

# declare constants
APP_NAME = "Auto Kunz Scrapper"
URL = "https://autokunz.ch/en/car-subscription/#filter=%7B%7D&sorting=a-z&page=1&display=abo&isAdvanced=false"
SOURCE = "auto_kunz"
counter = 0
cars = []

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/110.0',
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    'Content-Type': 'application/json',
    'Host': 'www.carvolution.com'
    }


def get_price(external_id, type, brand_name, vehicle_model, car_url):
    """
    
    :param external_id:
    :param type:
    :return:
    """
    url = f"https://autokunz.ch/en/buy/?car_id={external_id}&type={type}"
    # response = requests.get(url)
    response = call_limited_api(url)
    if response is None:
        beautify(SPACE, "Error", ": Response not found")
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        try:
            km = soup.find("input", attrs = {"name": "desired_km_per_year"})["value"]
            price = \
                soup.find("div", attrs = {"class": "col-5 pl-0 text-right h4 color-primary pt-3 nowrap"}).text.split()[
                    1].split(".")[0]
            # km = soup.find("div", attrs= {"class": "col-4 text-left"}).text
            price = int("".join("".join(price.split(".")).split("’")).split(",")[0])
            json_data = {
                "external_id": int(external_id),
                "source": str(SOURCE),
                "brand": str(brand_name),
                "model": str(trim_model_name(vehicle_model, brand_name)),
                "km": int(km),
                "price": float(price),
                "n_months": 1,
                "url": car_url
                }
            # print(json_data)
            if price != '' and price is not None:
                cars.append(json_data)
        except (RuntimeError, TypeError, NameError) as e:
            return None, None


def get_car_data(car_div):
    """
    
    :param car_div:
    :return:
    """
    vehicle_model = car_div.find("h6", attrs = {"class": "car-box__title"}).text
    external_id = car_div.find("a", attrs = {"class": "favorite-button"})["data-id"]
    car_url = car_div.find("a")["href"]
    global counter
    counter = counter + 1
    print("Counter - ", counter, " ", external_id)
    # response = requests.get(car_url)
    response = call_limited_api(car_url)
    # print(response.status_code, " - ", car_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        meta = soup.find("meta", attrs = {"name": "description"})
        brand_name = meta["content"].split(",")[0]
        if brand_name.lower() in CONFLICT_BRAND:
            brand_name = CONFLICT_BRAND[brand_name.lower()]
        all_thread = []
        for type in ["abo", "abo_aktion", "abo_aktion_2800"]:
            thread = threading.Thread(target = get_price, args = (external_id, type, brand_name, vehicle_model, car_url))
            thread.start()
            all_thread.append(thread)
        for thread in all_thread:
            thread.join()


def get_all_car_details(page_no):
    """
    
    :param page_no:
    :return:
    """
    print("page no - ", page_no)
    url = "https://autokunz.ch/wp/wp-admin/admin-ajax.php"
    payload = {
        'action': 'car_search',
        'page': page_no,
        'isIframe': 'false',
        'isAdvanced': 'true',
        'sorting': 'a-z',
        'default_filter': 'abo',
        'default_view': 'autos',
        'display_type': 'abo'
        }
    # response = requests.post(url, data = payload)
    response = call_limited_api(url, use_scrape_api_client = False, data = payload)
    # print(response.status_code, " - ", url)
    if response.status_code == 200:
        
        json_data = json.loads(response.text)
        soup = BeautifulSoup(json_data["content"], "html.parser")
        pages = json_data["pages"]
        if pages == 0:
            return
        all_car_div = soup.find_all("div", attrs = {"class": "car-box h-100 d-flex flex-column"})
        # print(len(all_car_div))
        all_thread = []
        for carDiv in all_car_div:
            thread = threading.Thread(target = get_car_data, args = (carDiv,))
            thread.start()
            all_thread.append(thread)
        
        for thread in all_thread:
            thread.join()
    else:
        beautify(SPACE, "Error", ": Page not found")
    get_all_car_details(page_no + 1)


def main():
    """
    
    :return:
    """
    # Application start display
    start_time = datetime.now()
    # Application start display
    start_display(APP_NAME, start_time)
    
    existing_cars_df = get_existing_car_list(SOURCE)
    
    beautify(SPACE, "Existing cars in database", ": " + str((existing_cars_df.shape[0])))
    
    beautify(SPACE, "Stage", ": Scrapping Cars\n")
    get_all_car_details(1)
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
