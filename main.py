import mysql.connector
import pandas as pd
import requests
import datetime
from prettytable import PrettyTable


def search(minScrapeTime, maxScrapeTime, keyword, size):
    cnx = mysql.connector.connect(user='admin', password='test12345',
                                  host='database-5.ctakbh4whue4.us-east-1.rds.amazonaws.com', database='mainDB')
    cursor = cnx.cursor()
    query = (
        "SELECT * FROM images WHERE scrapeTime BETWEEN %s AND %s AND keyword = %s ORDER BY scrapeTime DESC LIMIT %s")
    cursor.execute(query, (minScrapeTime, maxScrapeTime, keyword, size))
    results = cursor.fetchall()
    df5 = pd.DataFrame(results, columns=['imageUrl', 'scrapeTime', 'keyword'])
    cursor.close()
    cnx.close()
    return df5


def print_df(df5):
    x = PrettyTable(max_width=300)
    x.field_names = df5.columns
    for row in df.itertuples():
        x.add_row(row[1:])
    print(x)


def scrape(keyword, size):
    cnx = mysql.connector.connect(user='admin', password='test12345', host='database-5.ctakbh4whue4.us-east-1.rds'
                                                                           '.amazonaws.com', database='mainDB')
    cursor = cnx.cursor()
    response = requests.get("https://api.flickr.com/services/rest/?method=flickr.photos.search&api_key"
                            "=cd0babe37678818933c7095aae7aa07b&text=" + keyword + "&per_page=" + size +
                            "&format=json&nojsoncallback=1")
    data = response.json()
    photos = data["photos"]["photo"]
    for photo in photos:
        url = "https://farm" + str(photo["farm"]) + ".staticflickr.com/" + str(photo["server"]) + "/" + str(photo["id"]) + "_" + str(photo["secret"]) + ".jpg"
        scrape_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        query = "INSERT INTO images (imageUrl, scrapeTime, keyword) VALUES (%s, %s, %s)"
        cursor.execute(query, (url, scrape_time, keyword))
        cnx.commit()
    cursor.close()
    cnx.close()


df = search("2023-01-24 14:04:48", "2023-01-24 16:18:00", "house", 4)
print_df(df)
