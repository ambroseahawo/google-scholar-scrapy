import requests
from bs4 import BeautifulSoup
import csv
import time
import random
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import RequestException

def create_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    return session

def main():
    base_url = "https://scholar.google.com/scholar"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'}
    proxies = {"http": "http://username:password@us-ca.proxymesh.com:31280"}
    queries = ["technical indicators predict stock price", "momentum features for stock return factors", "geometric shapes technical indicators stock market", "price and volume data for stock market", "price based indicators stock prediciton"]
    session = create_retry_session()

    with open('google_scholar_results.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['query', 'title', 'url', 'citation_count'])
        writer.writeheader()
        print("CSV file opened and header written.")
        
        for query in queries:
            if not query:  # if query is an empty string, skip it
                continue
            
            print(f"Starting new query: {query}")

            for page in range(20):  # 100 pages, 10 results per page = 1000 results
                start = page * 10
                params = {"q": query, "start": start}

                # Randomly sleep between requests
                time.sleep(random.uniform(1, 5))

                try:
                    response = session.get(base_url, headers=headers, params=params, proxies=proxies)
                    response.raise_for_status()  # if the request is successful, no exception will be raised
                    print(f"Successfully fetched page {page} for query '{query}'")
                except RequestException as e:
                    print(f"An error occurred: {e}")
                    continue

                soup = BeautifulSoup(response.content, "html.parser")

                results = []

                for item in soup.select(".gs_r"):
                    result = {}
                    title_tag = item.find("h3", class_="gs_rt")
                    if title_tag:
                        title = title_tag.get_text(strip=True)
                        result['title'] = title

                        a_tag = title_tag.find("a")
                        if a_tag:
                            result['url'] = a_tag['href']

                    citation_tag = item.find("a", string=lambda x: x and "Cited by" in x)
                    if citation_tag:
                        result['citation_count'] = citation_tag.get_text(strip=True).split(" ")[-1]

                    result['query'] = query
                    results.append(result)

                writer.writerows(results)
                print(f"Results for page {page} of query '{query}' written to CSV.")
                
        print("All queries completed.")

if __name__ == "__main__":
    main()