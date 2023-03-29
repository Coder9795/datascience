"""

Clyde Information Scrapper

"""

from bs4 import BeautifulSoup

# local imports
from competitor_common import *

# declare constants
APP_NAME = "Clyde Scrapper"
URL = "https://clyde.ch/en/subscription?amp%3Bmileage=1000km&amp%3Bsorting=BASE_PRICE_ASC&duration=3_mt&mileage=500km&sorting=BASE_PRICE_ASC"
SOURCE = "clyde"
cars = []


def get_all_car_details(url):
    """
    
    :param url:
    :return:
    """
    # response = call_limited_api(url)
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        script = soup.find('script', attrs = {'id': '__NEXT_DATA__'}).string
        script_data = json.loads(script)
        props = script_data["props"]["pageProps"]
        if props:
            serialized_store = props["vehiclesGrouped"]
            if serialized_store:
                vehicles_grouped = []
                for model in serialized_store:
                    vehicles_grouped = vehicles_grouped + serialized_store[model]
                for vehicle in vehicles_grouped:
                    brand = vehicle["brandName"]
                    if brand.lower() in CONFLICT_BRAND:
                        brand = CONFLICT_BRAND[brand.lower()]
                    model = vehicle["modelName"]
                    model = trim_model_name(model, brand)
                    price_map = vehicle["priceMap"]
                    for kmMonth in price_map:
                        km, month = kmMonth.split("_")
                        price = price_map[kmMonth]
                        json_data = {
                            "external_id": vehicle["id"],
                            "source": SOURCE,
                            "brand": brand,
                            "model": model,
                            "km": km,
                            "n_months": month,
                            "price": price,
                            "url": "https://clyde.ch/de/unsere-autos/" + brand.lower() + "/" + model.lower().replace(" ", "-") + "/" + vehicle["id"]
                            }
                        if price != '' and price is not None:
                            cars.append(json_data)
    
    else:
        beautify(SPACE, "Error", ": Page not found")


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
