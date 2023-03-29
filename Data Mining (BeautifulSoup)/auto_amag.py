"""

Auto Amag information Scrapper

"""

import threading

from bs4 import BeautifulSoup

from competitor_common import *

cars = []
counter = 0

# declare constants
APP_NAME = "Auto Amag Scrapper"
URL = "https://auto.amag.ch/ajax-calls/threeSixtySearch/"
SOURCE = "auto_amag"
counter = 0
start_end = "2013,2023"


def get_km_list(external_id):
    """
    
    :param external_id:
    :return:
    """
    url = f"https://www.carmato.io/auto_abo/get_km_options/?vin={external_id}"
    response = requests.get(url)
    # response = call_scraper_api(url)
    if response is None:
        beautify(SPACE, "Error", ": Response not found")
    if response.status_code == 200:
        json_data = json.loads(response.text)
        return json_data["options"]


def get_month_list_for_each_km(external_id, km):
    """
    
    :param external_id:
    :param km:
    :return:
    """
    url = f"https://www.carmato.io/auto_abo/get_term_options/?vin={external_id}&km={km}"
    # response = call_scraper_api(url)
    response = requests.get(url)
    if response is None:
        beautify(SPACE, "Error", ": Response not found")
    if response.status_code == 200:
        json_data = json.loads(response.content.decode())
        return json_data["options"] if json_data["options"] else []


def get_price(external_id, km, month):
    """
    
    :param external_id:
    :param km:
    :param month:
    :return:
    """
    url = f"https://www.carmato.io/auto_abo/get_rate/?km={km}&vin={external_id}&dauer={month}"
    # response = call_scraper_api(url)
    response = requests.get(url)
    if response is None:
        beautify(SPACE, "Error", ": Response not found")
    if response.status_code == 200:
        json_data = json.loads(response.text)
        return json_data["rate"]


def get_each_car_data(car_div):
    """
    
    :param car_div:
    :return:
    """
    vin_number = car_div.find("div", attrs = {"class": "currentvin"}).text
    external_id = car_div.find("a", attrs = {"class": "product"})['data-uid']
    car_url = car_div.find("a", attrs = {"class": "product"})['href']
    global counter
    counter = counter + 1
    print("Counter - ", counter, " ", external_id)
    brand = car_div.find("a", attrs = {"class": "product"})["data-name"]
    model = car_div.find("a", attrs = {"class": "product"})["title"]
    model = trim_model_name(model, brand)
    if brand.lower() in CONFLICT_BRAND:
        brand = CONFLICT_BRAND[brand.lower()]
    km_list = get_km_list(vin_number)
    for km in km_list:
        month_list = get_month_list_for_each_km(vin_number, km)
        for month in month_list:
            price = get_price(vin_number, km, month)
            json_data = {
                "external_id": external_id,
                "source": SOURCE,
                "brand": brand,
                "model": model,
                "km": km,
                "n_months": month,
                "price": price,
                "url": "https://auto.amag.ch/" + car_url,
                "vin_number": vin_number
                }
            if price != '' and price is not None:
                cars.append(json_data)


def get_all_car_details(page_no):
    """
    
    :param page_no:
    :return:
    """
    data = {
        "oldAmount": 19,
        "search360[initial_registration_year]": start_end,
        "search360[price_customer]": "0,150000",
        "search360[auto_abo]": 1,
        "search360[page]": page_no,
        "search360[percent]": 0,
        "search360[current_miles]": "0,100000",
        "search360[power_hp]": "0,500",
        "search360[consumption]": "0,15",
        "search360[speed_up]": "0,15",
        "maps[mapsDistance]": "700",
        }
    
    payload = {
        'api_key': API_KEY,
        'url': URL
        }
    headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36"}
    
    # response = requests.post('http://api.scraperapi.com', headers = headers, params = payload, data = data )
    
    # response = call_limited_api(url = URL, headers = headers, data = data, use_scrape_api_client = False)
    
    response = requests.post(url = URL, data = data)
    
    if response is None:
        beautify(SPACE, "Error", ": Response not found")
    if response.status_code == 200:
        json_data = json.loads(response.text)
        soup = BeautifulSoup(json_data["html"], "html.parser")
        all_car_div = soup.find_all("div", attrs = {"class": "col-md-3 search-list-item"})
        if len(all_car_div) == 0:
            return 0
        all_thread = []
        for car_div in all_car_div:
            thread = threading.Thread(target = get_each_car_data, args = (car_div,))
            thread.start()
            all_thread.append(thread)
        for thread in all_thread:
            thread.join()
    else:
        beautify(SPACE, "Error", ": Unable to find the page")


def main():
    """
    
    :return:
    """
    start_time = datetime.now()
    # Application start display
    start_display(APP_NAME, start_time)
    
    existing_cars_df = get_existing_car_list(SOURCE)
    
    beautify(SPACE, "Existing cars in database", ": " + str((existing_cars_df.shape[0])))
    
    page_no = 1
    
    beautify(SPACE, "Stage", ": Scrapping Cars\n")
    while True:
        response = get_all_car_details(page_no)
        if response == 0:
            beautify(SPACE, "Status", ": Completed Successfully")
            beautify(SPACE, "Pages Scrapped", f": {page_no}")
            # Application end display
            break
        page_no += 1
    
    beautify(SPACE, "Stage", ": Data transfer to mysql \n")
    beautify(SPACE, "Inserting data to mysql", ": " + str(len(cars)))
    new_cars_df = insert_into_table_competitors(cars, SOURCE)
    beautify(SPACE, "\nStatus", ": Completed Successfully")
    
    removed_car_df = existing_cars_df[~existing_cars_df.apply(tuple, 1).isin(new_cars_df.apply(tuple, 1))]
    
    beautify(SPACE, "Updating removed_car_list in mysql", ": " + str((removed_car_df.shape[0])))
    
    to_update = json.loads(removed_car_df.to_json(orient = 'records'))
    
    update_removed_records(to_update)
    
    end_display(start_time)


if __name__ == "__main__":
    main()
