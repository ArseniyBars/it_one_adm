import sys
import os
import asyncio
import json
from aio_pika import Message, connect

import pandas as pd
import zipfile
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from numpy.random import normal 

from config import conn  

proxy=sys.argv[1]
chrome_options = webdriver.ChromeOptions()
chrome_options.add_extension(os.path.join('proxy_extensions', proxy+'.zip'))
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), 
							options=chrome_options)

def discover(tids, latitude, longitude):
    link=f"https://yandex.ru/maps/213/moscow/?ll={longitude}%2C{latitude}&mode=whatshere&whatshere%5Bpoint%5D={longitude}%2C{latitude}&whatshere%5Bzoom%5D=17.93&z=17.93"
    driver.get(link)
    adress_xpath='//div[@class="toponym-card-title-view__description"]'
    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, adress_xpath)))
    driver.implicitly_wait(0.4)
    element=driver.find_element(By.XPATH, adress_xpath)
    main_adress=element.text
    print(main_adress)
    transport_xpath='//div[@aria-label="Как добраться"]'
    try:
        element=WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, transport_xpath)))
        ActionChains(driver).move_to_element(driver.find_element(By.XPATH, transport_xpath)).perform()
    except:
        metro=[]
    else:
        metro=element.find_elements(By.XPATH, '//div[@aria-label="Метро рядом"]')
    if metro!=[]:
        metro_station=metro[0].find_element(By.XPATH, 
                                            '//div[@class="masstransit-stops-view__stop-name"]').text
        metro_distance=metro[0].find_element(By.XPATH, 
                                             '//div[@class="masstransit-stops-view__stop-distance"]').text
        print('\tМетро:', metro_station, metro_distance)
    else:
        metro_station, metro_distance=(None, None)
    pd.DataFrame([[tids, latitude, longitude, link, main_adress, metro_station, metro_distance]]
            ).to_csv('adm_parsing_data.csv', index=False, header=False, mode='a')
    time_to_wait=normal(1.2, 1.2, 1)[0]
    driver.implicitly_wait(time_to_wait if time_to_wait>0 else 0.1)
    orgaizations_button=driver.find_elements(By.XPATH, 
                                             '//div[contains(@aria-label, "Организации внутри")]')
    if orgaizations_button!=[]:
        orgaizations_button[0].click()
        driver.implicitly_wait(0.4)
        organization_list=driver.find_elements(By.XPATH, 
                '//div[@class="card-businesses-list__list"]//div[@class="search-business-snippet-view"]')
        if organization_list!=[]:
            print("\tОрганизации: ")
        to_frame=[]
        for org in organization_list:
            org_name=org.find_element(By.XPATH, 
                                      './/div[@class="search-business-snippet-view__title"]').text
            org_category=org.find_elements(By.XPATH, 
                                          './/div[@class="search-business-snippet-view__categories"]')
            org_category=None if org_category==[] else org_category[0].text
            print("\t\t", org_name, org_category)
            to_frame.append([tids, org_name, org_category])
        pd.DataFrame(to_frame).to_csv('adm_organizations.csv', index=False, header=False, mode='a')
    time_to_wait=normal(1.2, 1.2, 1)[0]
    driver.implicitly_wait(time_to_wait if time_to_wait>0 else 0.1)
	
async def main():
	connection = await connect(conn)
	async with connection:
		channel = await connection.channel()
		await channel.set_qos(prefetch_count=2)
		queue = await channel.declare_queue("parsing_input", durable=True)
		async for msg in queue:
			if msg.body.decode()=="quit":
				msg.ack()
				exit()
			try:
				discover(**(json.loads(msg.body.decode())))
			except Exception as e:
				msg.nack()
				await channel.default_exchange.publish( 
					Message(str(e).encode()), routing_key="parsing_output")
				raise
			else:
				msg.ack()
				await channel.default_exchange.publish( 
					Message("success".encode()), routing_key="parsing_output")
					
asyncio.run(main())
