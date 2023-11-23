import time, psycopg2, boto3, json
from datetime import date
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name="eu-west-1"
)
get_secret_value_response = client.get_secret_value(SecretId="RedshiftCon")
secret = get_secret_value_response['SecretString']
secret_dict = json.loads(secret)

connection = psycopg2.connect(
    database="postgres",
    user=secret_dict['awsRSu'],
    password=secret_dict['awsRSp'],
    host=secret_dict['awsRSep'],
    port='5432'
)
cursor = connection.cursor()

driver = webdriver.Chrome()
driver.implicitly_wait(10)

class Scraper:
    def scraper(self):
        duplicates = set()

        for category in self.Urls:
            print(category)
            data = []
            currentPage = self.startPageIndex
            CategoryUrl = self.Urls[category]

            while True:
                ## Connection Script
                time.sleep(1)
                attempt = 1
                while True:
                    try:
                        driver.get(CategoryUrl.format(pageNumber=currentPage))
                        break
                    except Exception as error:
                        attempt += 1
                        if attempt > 5:
                            print("Too many attempts to connect, aborting the script")
                            return
                        else:
                            print("Trying to connect again in 15 seconds")
                            time.sleep(15)
                
                time.sleep(3)
                products = driver.find_elements(By.CSS_SELECTOR, self.productListSelector)

                if len(products) < 2:
                    print(f'No products found on page {int(currentPage/self.pageIndexIteration)+1}, breaking loop')
                    print()
                    break
                
                for index,eachProduct in enumerate(products):
                    try:
                        productName = eachProduct.find_elements(By.CSS_SELECTOR, self.productNameSelector)[0].text
                        productPrice = eachProduct.find_elements(By.CSS_SELECTOR, self.productPriceSelector)[0].text
                        productPricePerMeasure = eachProduct.find_elements(By.CSS_SELECTOR, self.productPricePerKiloSelector)[0].text
                        todays_date = str(date.today().strftime("%d%m%y"))
                        print(productName,productPrice,productPricePerMeasure,index)
                        if productName not in duplicates:
                            duplicates.add(productName)
                            data.append((category,productName,productPrice,productPricePerMeasure,todays_date))
                    except:
                        pass
    

                currentPage += self.pageIndexIteration
                print(f"End of page {int(currentPage/self.pageIndexIteration)} of category {category}, total products = {len(duplicates)}")
                print()
                # break # Break after first page of each category for testing

            self.addToTable(data)

    def addToTable(self,data):
        ## Query 1 - Create Temp Table
        cursor.execute(f"""
            create global temp table temptable1 (
                category varchar(1000),
                productName varchar(2000),
                productPrice varchar(100),
                productPricePerMeasure varchar(100),
                todays_date varchar(100)
            ) ON COMMIT DROP""")
        print("Temp Table created")

        ## Query 2 - Add Data to Temp Table
        cursor.executemany(f"""insert into temptable1 
                (category, productName, productPrice, productPricePerMeasure, todays_date) 
                values(%s, %s, %s, %s, %s)""", data)
        print("Temp Insert Successfull")

        ## Query 3 - Copy Data from Temp Table to Main Table
        cursor.execute(f"""insert into {self.table} 
                        (category, productName, productPrice, productPricePerMeasure, todays_date) 
                        SELECT category, productName, productPrice, productPricePerMeasure, todays_date
                        FROM temptable1""")
        print("Temp table copy to main table successful")
        
        ## Commit Changes and Close
        connection.commit()
        print("Commit Successfull")
        print()


class SainsburysScraper(Scraper):
    def __init__(self):
        self.table = "SainsburysScrape"
        self.productListSelector = ".product"
        self.productNameSelector = ".productNameAndPromotions > h3 > a" 
        self.productPriceSelector = ".pricePerUnit"
        self.productPricePerKiloSelector = ".pricePerMeasure"
        self.Urls = {
            # "Bakery":"https://www.sainsburys.co.uk/shop/CategorySeeAllView?listId=&catalogId=10241&searchTerm=&beginIndex={pageNumber}&pageSize=120&orderBy=FAVOURITES_FIRST&top_category=&langId=44&storeId=10151&categoryId=12320&promotionId=&parent_category_rn=",
            #"Fruit_and_Veg":"https://www.sainsburys.co.uk/shop/CategorySeeAllView?listId=&catalogId=10241&searchTerm=&beginIndex={pageNumber}&pageSize=120&orderBy=FAVOURITES_FIRST&top_category=&langId=44&storeId=10151&categoryId=12518&promotionId=&parent_category_rn=",
            #"Meat_and_Fish":"https://www.sainsburys.co.uk/shop/CategorySeeAllView?listId=&catalogId=10241&searchTerm=&beginIndex={pageNumber}&pageSize=120&orderBy=FAVOURITES_FIRST&top_category=&langId=44&storeId=10151&categoryId=13343&promotionId=&parent_category_rn=",
            #"Chilled_and_Dairy":"https://www.sainsburys.co.uk/shop/CategorySeeAllView?listId=&catalogId=10241&searchTerm=&beginIndex={pageNumber}&pageSize=120&orderBy=FAVOURITES_FIRST&top_category=&langId=44&storeId=10151&categoryId=428866&promotionId=&parent_category_rn=", 
            #"Frozen":"https://www.sainsburys.co.uk/shop/CategorySeeAllView?listId=&catalogId=10241&searchTerm=&beginIndex={pageNumber}&pageSize=120&orderBy=FAVOURITES_FIRST&top_category=&langId=44&storeId=10151&categoryId=218831&promotionId=&parent_category_rn=",
            "Food_Cupboard":"https://www.sainsburys.co.uk/shop/CategorySeeAllView?listId=&catalogId=10241&searchTerm=&beginIndex={pageNumber}&pageSize=120&orderBy=FAVOURITES_FIRST&top_category=&langId=44&storeId=10151&categoryId=12422&promotionId=&parent_category_rn=",        
            "Drinks":"https://www.sainsburys.co.uk/shop/CategorySeeAllView?listId=&catalogId=10241&searchTerm=&beginIndex={pageNumber}&pageSize=120&orderBy=FAVOURITES_FIRST&top_category=&langId=44&storeId=10151&categoryId=12192&promotionId=&parent_category_rn=",
            "Household":"https://www.sainsburys.co.uk/shop/CategorySeeAllView?listId=&catalogId=10241&searchTerm=&beginIndex={pageNumber}&pageSize=120&orderBy=FAVOURITES_FIRST&top_category=&langId=44&storeId=10151&categoryId=12564&promotionId=&parent_category_rn="
        }
        self.startPageIndex = 0
        self.pageIndexIteration = 120

class ALDIScraper(Scraper):
    def __init__(self):
        self.table = "ALDIScrape"
        self.productListSelector = ".product-tile"
        self.productNameSelector = ".product-tile-text > a"  
        self.productPriceSelector = ".product-tile-price .h4 > span"
        self.productPricePerKiloSelector = ".product-tile-price .text-gray-small > span"
        self.Urls = {
            "Bakery":"https://groceries.aldi.co.uk/en-GB/bakery?&page={pageNumber}",
            "Fresh_Food":"https://groceries.aldi.co.uk/en-GB/fresh-food?&page={pageNumber}",
            "Drinks":"https://groceries.aldi.co.uk/en-GB/drinks?&page={pageNumber}",
            "Food_Cupboard":"https://groceries.aldi.co.uk/en-GB/food-cupboard?&page={pageNumber}",
            "Frozen":"https://groceries.aldi.co.uk/en-GB/frozen?&page={pageNumber}",
            "Chilled":"https://groceries.aldi.co.uk/en-GB/chilled-food?&page={pageNumber}",
            "Household":"https://groceries.aldi.co.uk/en-GB/household?&page={pageNumber}"
        }
        self.startPageIndex = 1
        self.pageIndexIteration = 1

SainsburysScraper().scraper()
ALDIScraper().scraper()

connection.close()