import requests
from bs4 import BeautifulSoup
from datetime import datetime
from pync import Notifier
import logging
import sqlite3
import os

'''
This program is designed to cycle through the trending deals 
from popular Canadian financial forum RedFlagDeals 
and notify once a new deal satisfying the engagement criteria.

The engagement criteria is whether or not the ratio of 
upvotes / replies is greater than 2, which is my personal
weighting, tuned after some trial and error.

This is intended to be run as a cron job, on a time interval
that fits desired notification tolerance.
'''

# Configuration
folder_path = os.path.expanduser("~/Python/Scraping/RFD")
db_path = os.path.join(folder_path, "deals.db")
log_path = os.path.join(folder_path, "RFDlogfile.log")
url = "https://forums.redflagdeals.com/hot-deals-f9/trending"
logging.basicConfig(filename=log_path, level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Set up notification function
def send_notification(title, message, url):
    try:
        Notifier.notify(message, title=title, open=url)
    except Exception as e:
        logging.error(f"Notification Error: {e}")

# SQL Database setup
def setup_database():
    os.makedirs(folder_path, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT UNIQUE,
                upvotes INTEGER,
                replies INTEGER,
                ratio REAL,
                url TEXT
        )
        ''')
        conn.commit()
        return conn

# Cleans up old deals to maintain a manageable size
def cleanup_old_deals(conn):
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM deals")
        count = cursor.fetchone()[0]
        
        # If the database exceeds 300 entries, delete the oldest 50
        if count > 300:
            cursor.execute('''
                DELETE FROM deals WHERE id IN (
                    SELECT id FROM deals ORDER BY id LIMIT 50
                )
            ''')
            conn.commit()
            logging.info("Deleted oldest 50 deals to maintain database size.")

# Deal Scraper
def deal_scraper(url, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT title FROM deals")
    seen_titles = {row[0] for row in cursor.fetchall()}
    
#Access the page
    try:
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching URL: {e}")
        return

# Parse the page
    soup = BeautifulSoup(response.text, 'html.parser')
    threads_container = soup.find('ul', class_='topiclist topics trending with_categories')
    threads = threads_container.find_all('div', class_='thread_main')
    if threads:
        logging.info(f"Found {len(threads)} threads")  
    else:
        logging.info("No threads found")
    
    new_deals_found = False

    for thread in threads:
        
        dealtitle = thread.find('a', class_='thread_title_link').get_text(strip=True) if thread.find('a', class_='thread_title_link') else None
        deal_url = "https://forums.redflagdeals.com" + thread.find('a', class_='thread_title_link')['href'] if dealtitle else None
        upvotes = int(thread.find('div', class_='votes thread_stat').find('span').text.strip().replace(',', '')) if thread.find('div', class_='votes thread_stat') else 0
        replies = int(thread.find('div', class_='posts thread_stat').find('span').text.strip().replace(',', '')) if thread.find('div', class_='posts thread_stat') else 0
        ratio = upvotes / replies if replies > 0 else float('inf')
        
        if dealtitle :
            logging.debug(f"Found deal title: {dealtitle}")
        else:
            logging.debug("Deal title is missing")
            
        if dealtitle and any(keyword in dealtitle for keyword in ["Dollarama", "Costco West", "PC Optimum"]):
            logging.info(f"Skipping deal: {dealtitle} (contains filtered keywords)")
            continue
            
        # When upvotes and replies are found or missing
        if upvotes:
            logging.debug(f"Found upvotes: {upvotes}")
        else:
            logging.debug("Upvotes are missing")

        if replies:
            logging.debug(f"Found replies: {replies}")
        else:
            logging.debug("Replies are missing")

        if ratio:
            logging.debug(f"Found ratio: {ratio}")
        else:
            logging.debug("Ratio is missing")

        if dealtitle and upvotes and replies:
            try:
                logging.info(f"Processed deal: Title='{dealtitle}', Upvotes={upvotes}, Replies={replies}")
                
            except ZeroDivisionError as zde:
                logging.error(f"ZeroDivisionError: {zde}")
            except ValueError as ve:
                logging.error(f"ValueError on upvotes/replies conversion: {ve}")
                
# Check criteria
        if ratio > 2 and dealtitle not in seen_titles:
                seen_titles.add(dealtitle.strip())
                    
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO deals (title, upvotes, replies, ratio, url)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (dealtitle, upvotes, replies, ratio, deal_url))
                    conn.commit()
                
                    send_notification(
                    title="New Deal!",
                    message=f"Title: {dealtitle.strip()}\nUpvotes: {upvotes}\nReplies: {replies}\nRatio: {ratio:.2f}",
                    url=deal_url)
                    
                    new_deals_found = True

                    # Call cleanup function to ensure database size is managed
                    cleanup_old_deals(conn) 

                except sqlite3.Error as db_err:
                    logging.error(f"Database Insertion Error: {db_err}")
                
    if not new_deals_found:
        logging.info(f"No new deals found at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        logging.info("\n=== Hot Deals Sorted by Ratio ===\n")
        cursor.execute("SELECT title, upvotes, replies, ratio, url FROM deals ORDER BY ratio DESC")
        deals = cursor.fetchall()
        for index, deal in enumerate(deals, start=1):
            logging.info(f"Deal {index}:")
            logging.info(f"  Title  : {deal[0]}")
            logging.info(f"  Upvotes: {deal[1]}")
            logging.info(f"  Replies: {deal[2]}")
            logging.info(f"  Ratio  : {deal[3]:.2f}")
            logging.info(f"  Link   : https://forums.redflagdeals.com{deal[4]}")
            logging.info(" " + "-" * 40)

# Running the program
conn = setup_database()
deal_scraper(url, conn)