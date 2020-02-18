# coding: utf-8

import pandas as pd
import yaDirect
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Импорт файла с настройками
from settings import *
# Файл настроек содержит следующие переменные:
# ссылка на таблицу с минус словами
# SPREADSHEETS = "https://docs.google.com/spreadsheets/d/1yWCDVbfw6eS3NXj6BVhSmGiBCj-s5eiYFII0I72JWMw/edit?usp=sharing"

# Файл с ключами для Google API
# KEYFILE = "vse-v-dom-6aac6b76ede1.json"

# Перечень управляющих аккаунтов Яндекс Директа и токенов для них
# ALL_TOKEN = {
#    "context.elema":{ "token":"AgAAAAAcXXXXXXXXXn0I9Ul66Eh0",
#                             "description":"Аккаунт 1"},
   
#     "context-elema1":{"token":"AgAAAAAmEXXXXXXXXXXXkcqnslaluHf2DE",
#                                 "description":"Аккаунт 2"}   
#              }



# Ссылка на таблицу со списками минус слов
# Первая страница должна быть с перечнем кампаний и называться Campaign
# Пример файла по ссылке https://docs.google.com/spreadsheets/d/1yWCDVbfw6eS3NXj6BVhSmGiBCj-s5eiYFII0I72JWMw/edit?usp=sharing


# Если нужно получить весь список кампаний клиентских аккаунтов запускаем эту функцию, данные сохранятся в файл campaign_list.csv
def get_all_clients():
    all_clients = pd.DataFrame()
    for account in ALL_TOKEN:
  
        direct = yaDirect.yaDirect(ALL_TOKEN[account]['token'], account )
        client = pd.DataFrame(direct.get_AgencyClients())
        client['mcc_account'] = account
        all_clients = all_clients.append(client)
    all_clients = all_clients[all_clients.Archived == 'NO']
    
    all_campaigns = pd.DataFrame()
    
    for index, row in all_clients.iterrows():
        print ("login: ", row['Login'])
        direct = yaDirect.yaDirect(ALL_TOKEN[row['mcc_account']]['token'], row['Login'] )
        params = {  "SelectionCriteria": { 
            "Types": [ "TEXT_CAMPAIGN" ],
            "States": [ "ON" ]
         },
            "FieldNames": ["Id", "Name"]

            }
        result = direct.get_Campaigns(params)
        try:
            campaigns = pd.DataFrame(result['result']['Campaigns'])
            campaigns["mcc_account"] = row['mcc_account']
            campaigns["Login"] = row['Login']
            all_campaigns = all_campaigns.append(campaigns, ignore_index=True) 
        except:
            print ("Кампании не найдены")


      
    result = all_campaigns[['mcc_account','Login','Id','Name']]
    result.to_csv('campaign_list.csv', index=False)

# Получение минус слов из кампании
def get_minus_from_one_account (accaunt):
    mcc_login = accaunt[0]
    login = accaunt[1]
    campaign_id = accaunt[2]

    direct = yaDirect.yaDirect(ALL_TOKEN[mcc_login]['token'], ALL_TOKEN[mcc_login]  )
    direct.set_Login(login)
    params = {  "SelectionCriteria": { 
    "Ids": [campaign_id]
    },
            "FieldNames": ["Id", "Name", "NegativeKeywords"]
            }

    camps = direct.get_Campaigns( params )
    camps = camps.get('result').get('Campaigns')
    if camps is not None:
        NegativeKeywords = camps[0].get('NegativeKeywords')
        if NegativeKeywords is not None:
            return NegativeKeywords.get('Items')
        else:
            return []
    else:
        print ("ERROR: No active campains ({})".format(camps) )
        return []


# Выгружаем минус слова в настройки кампании
def update_negative_key(accaunt, neg_key):
    mcc_login = accaunt[0]
    login = accaunt[1]
    campaign_id = accaunt[2]
    params = {
            "Campaigns": [{
                "Id": campaign_id ,
                "NegativeKeywords": { "Items": neg_key }
                }]
            }
    
    direct = yaDirect.yaDirect(ALL_TOKEN[mcc_login]['token'], ALL_TOKEN[mcc_login]  )
    direct.set_Login(login)
    return direct.update_Campaigns(params=params)


# -------------------- Основная часть -----------------------
def main():
    scope = ["https://spreadsheets.google.com/feeds",
    'https://www.googleapis.com/auth/spreadsheets',
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"]

    creds = ServiceAccountCredentials.from_json_keyfile_name(KEYFILE, scope)

    client = gspread.authorize(creds)

    sheets = client.open_by_url(SPREADSHEETS) 
    #Формируем список используемых наборов минус слов в  документе
    campaigns_list = sheets.worksheet('Campaign').get_all_values()
    minus_sheets_names = [ loc[4] for loc in campaigns_list[1:] ]
    minus_sheets_names = list(set(minus_sheets_names))
    time.sleep(1)
    #Общий список минус слов со всех листов, используемых на первом листе
    minus_list = {}
    for sheet in minus_sheets_names:
        minus_list[sheet] = sheets.worksheet(sheet).col_values(1)
        time.sleep(1)

    #Проходим по кампаниям
    for accaunt in campaigns_list[1:]:
        minus = []
        minus = get_minus_from_one_account (accaunt)

        #Дополняем список из кампании списком из таблицы по листу из 5й колонки
        minus.extend(minus_list[accaunt[4]])
        #Оставляем уникальные значения
        minus = list(set(minus))
        res = update_negative_key(accaunt, minus).get("result").get("UpdateResults")
        #Загружаем обратно в кампанию
        print("mcc_login={mcc} , login={login} , campain_id= {id} , campaign_name= {name} , \
n_key_list={nlist} , result={res}".format(
             mcc=accaunt[0], login=accaunt[1], id=accaunt[2], name=accaunt[3], 
             nlist=accaunt[4], res=res)
             )





if __name__ == '__main__':
    main()
    #get_all_clients()

