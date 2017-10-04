from bs4 import BeautifulSoup
import urllib.robotparser
import urllib.request
import networkx as nx
import urllib.error
import webbrowser
import requests
import argparse
import queue
import json
import csv
import sys
import re

def load_api():
    with open("./HW1/search_ID", 'r') as f:
        reader = f.read()
        search_ID = reader[:-1]
    with open("./HW1/search_key", 'r') as f:
        reader = f.read()
        search_key = reader[:-1]
    return search_ID, search_key

def load_request():
    '''load google request for query1'''
    s = []
    with open('string.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            s.append(row)
    s = s[0]
    return s

class Robot:
    
    def __init__(self):
        
        self.warning = {}
    
    def check_robot(self, site, link):
        '''
        input: string
        actoin: check if site in robot.txt and update list of permission
        output: bool
        '''
        try:
            if site not in self.warning:
                self.warning[site] = urllib.robotparser.RobotFileParser()
            ur = "https://" + site
            url = ur + "/robots.txt"
            self.warning[site].set_url(url)
            self.warning[site].read()
            logic = self.warning[site].can_fetch("*", link)
        except:
            logic = True
        return logic

class Crawler:
    
    '''run a crawler and every 10% iterations compute a partial pagerank and crawl starting from the highest ranked link'''
    
    def __init__(self, string, search_key, search_ID, start, limit):
        
        '''
        input: 
        : string(string) - search query
        : search_key(string) - google API key
        : search_ID(string) - ID google application
        : start(list) - if [] request to google else use submitted links
        : limit(int) - max number of pages crawable from the same site
        '''
        
        self.string = string
        self.limit = limit
        self.search_key = search_key
        self.search_ID = search_ID
        self.url = " "
        self.starting_url = start
        self.sites = {}
        self.dict_url = {}
        self.url_dict = {}
        self.visited = {}
        self.pageranks = {"partial_pg": {}, "pg": {}}
        self.direct_graph = {}   ## outgoing
        self.inverted_graph = {} ## ingoing
        self.frontier = queue.PriorityQueue(maxsize=10000)
        
        self.robot = Robot()
        
        if self.starting_url == []:
            print("request google")
            self.search_engine_request()
            
        self.initialize()
        
    def get_starting_list(self):
        return self.starting_url
    def get_dict_url(self):
        return self.dict_url
        
    def search_engine_request(self):
        '''
        input:  None 
        action: request to google 10 results
        output: list with 10 href
        '''
        try:
            string = self.string.split(" ")
            new_string = string[0]
            for i in range(1, len(string)):
                s = string[i]
                new_string += "+" + s

            self.url = "https://www.googleapis.com/customsearch/v1?key=" + self.search_key + "&cx=" + self.search_ID + "&q=" + new_string
            r = urllib.request.urlopen(self.url)
            html = r.read()

            soup = BeautifulSoup(html, 'html.parser')
            page = json.loads(str(soup))

            for e in page["items"]:
                self.starting_url.append(e["link"])
                
            r.close()      
        except urllib.error.HTTPError as e:
            print(e.getcode())
            print("check IP address")
    
    def check_site(self, link):
        '''
        input:  string
        action: check if this site is been already crawled > limit and link not in robot.txt 
        output: bool
        '''
        try:
            logic=True
            original_link = link
            link = link.split("://")
            if len(link) > 1:
                link = link[1]
                site = link.split("/")[0]
            else:    
                site = "www.google.com" # link[0]
            
            if site not in ["www.google.com"]:
                if site not in self.sites:
                    self.sites[site] = 0
                    
                logic = self.robot.check_robot(site, original_link)
                self.sites[site] += 1
                if self.sites[site] > self.limit:
                    logic=False    
        except ConnectionError:
            print("error")
            logic=False
        
        return logic
    
    def crawl(self, url):
        '''
        input: string
        action: crawl the url and extract links
        output: list
        '''
        status = -1
        links = []
        try:
            r = requests.get(url)
            status = r.status_code
        except InvalidURL:
            return links
        
        if status != requests.codes.ok:
            return links
            
        html = r.text
        soup = BeautifulSoup(html, "lxml")
        links = set()
        href = soup.find_all('a')
        for element in href:
            try:
                link = element["href"]
                logic = self.check_site(link)
                if logic:
                    if link.split("://")[0] in ["http", "https"]:
                        links.add(link)
                    #else:
                    #    link = "http://www.google.com" + link
                    #    if link not in self.visited:
                    #        links.append(link)
            except KeyError:
                continue
                
        links = list(links)
        return links
    
    
    def compute_page_rank(self):
        graph = nx.DiGraph(self.direct_graph)
        pg = nx.pagerank(graph, max_iter=100)
        return pg
    
    def update_frontier(self, pg):
        '''
        input: pagerank output (node, score)
        action: update the priority queue with the new ranking
        output: None
        '''
        self.frontier = queue.PriorityQueue(maxsize=10000)
        for key in pg:  
            if key in self.dict_url:
                if self.dict_url[key] not in self.visited:
                    a = (pg[key], key)
                    self.frontier.put(a)
                if key not in self.pageranks["partial_pg"]:
                    self.pageranks["partial_pg"][key] = pg[key]
                
    def update_pagerank(self, pg):
        for key in pg:
            if key not in self.pageranks["partial_pg"]:
                self.pageranks["partial_pg"][key] = pg[key]
            self.pageranks["pg"][key] = pg[key]
    
    def initialize(self):
        '''
        input: None
        action: initialize the data structures
        output: None
        '''
    
        n = len(self.starting_url)
        
        self.dict_url = {0: self.url}
        self.url_dict = {self.url: 0}
        self.visited = {self.url: 0}
        self.direct_graph = {0: [i for i in range(1, n + 1)] }
        self.inverted_graph = {i: [0] for i in range(1, n + 1) } 
        self.inverted_graph[0] = []

        for i in range(0, n):
            url_i = i + 1
            url = self.starting_url[i]
            self.dict_url[url_i] = url
            self.url_dict[url] = url_i

            a = tuple((0, url_i))
            self.frontier.put(a)
        print("initialized")
            
            
    def outgoing(self):
        '''
        input: None
        action: compute outgoing urls from the highest ranked link in frontier
        output: list of urls and index highest ranked link
        '''
        outgoing_links = []
        if not self.frontier.empty():
            ix_parent = self.frontier.get()[1]
            link = self.dict_url[ix_parent]
            if link not in self.visited:
                # crawling
                outgoing_links = self.crawl(link)
                self.visited[link] = ix_parent

        return outgoing_links, ix_parent
    
    
    def update(self, outgoing_links, ix_parent):
        '''
        input: list of links and index url
        action: update all the data structures
        output: None
        '''
        #print(outgoing_links)
        if ix_parent not in self.direct_graph:
                self.direct_graph[ix_parent] = []
        ix_child = len(self.dict_url)
                    
        for element in outgoing_links:
            if element not in self.visited:
                if element not in self.url_dict:
                    # update
                    self.dict_url[ix_child] = element
                    self.url_dict[element] = ix_child
                    a = tuple((0, ix_child))
                    self.frontier.put(a)
                    
                    # outgoing
                    self.direct_graph[ix_parent].append(ix_child)
                    if ix_child not in self.inverted_graph:
                        self.inverted_graph[ix_child] = []
                    # ingoing
                    self.inverted_graph[ix_child].append(ix_parent)
                    ix_child += 1
                else:
                    ix = self.url_dict[element]
                    self.direct_graph[ix_parent].append(ix)
                    self.inverted_graph[ix].append(ix_parent)    
            else:
                ix = self.url_dict[element]
                self.direct_graph[ix_parent].append(ix)
                self.inverted_graph[ix].append(ix_parent)
                
    def run(self):
        
        outgoing_links, ix_parent = self.outgoing()
        self.update(outgoing_links, ix_parent)
        
        
    def write(self):
        with open('href.csv', 'w') as f:
            writer = csv.writer(f)
            
            for key in range(1, len(self.dict_url)):
                f.write(str(self.dict_url[key]) + " ;")
                f.write(str(self.pageranks["partial_pg"][key]) + " ;")
                f.write(str(self.pageranks["pg"][key]) + "\n")
        print("All Done.")
        
    def save_crawler(self):
        
        crawler = {"dict_url": self.dict_url, "direct_graph": self.direct_graph, "inverted_graph": inverted_graph ,
                   "visited": self.visited, "frontier": self.frontier}
        with open('crawler.json', 'w') as f:
            json.dump(crawler, f)



def main(string, search_key, search_ID, N=1000, start = [], limit=20, flag=False):
    
    '''
    input:
    : string - query
    : start - if [] request to google else use submitted list of links
    action: run a crawler and every 10% iterations compute a partial pagerank and crawl starting from the highest ranked links
    output: crawler
    '''

    crawler = Crawler(string, search_key, search_ID, start, limit)
    loop = 0
    try:
	    while len(crawler.dict_url) < N and not crawler.frontier.empty():
	        
	        loop += 1
	        crawler.run()
	        
	        if loop % 40 == 0:
	            print("pagerank")
	            pg = crawler.compute_page_rank()
	            crawler.update_frontier(pg)

	        if loop % 10 == 0:
	            print("ok")
	            print(loop)
	            print( "visited: ", len(crawler.visited) )
	            print( "dict_url: ", len(crawler.dict_url) )
	            print( "frontier: ", crawler.frontier.qsize() )
	            print("direct: ", len(crawler.direct_graph) )
	            print("invert: ", len(crawler.inverted_graph) )
	            
	    pg = crawler.compute_page_rank()
	    crawler.update_pagerank(pg)
	    if flag:
	        crawler.write()
	        crawler.save_crawler()
	            
	    return crawler

	except KeyboardInterrupt:
		print("stop")
		print(loop)
        print( "visited: ", len(crawler.visited) )
        print( "dict_url: ", len(crawler.dict_url) )
        print( "frontier: ", crawler.frontier.qsize() )
        print("direct: ", len(crawler.direct_graph) )
        print("invert: ", len(crawler.inverted_graph) )

if __name__ == "__main__":

	query="ebbets field"
	
	try:
		search_id, search_key = load_api()
	except:
		None

	parser = argparse.ArgumentParser(description='crawler with incremental pagerank')
	parser.add_argument('-q',  '--query',     dest='query', action='store', default=query)
	parser.add_argument('-id', '--search_id', dest='search_id', action='store', default=search_id)
	parser.add_argument('-k',  '--searc_key', dest='search_key', action='store', default=search_key)
	parser.add_argument('-n',  '--number',    dest='N', action='store', default=1000)
	parser.add_argument('-s',  '--start',     dest='s', action='store_true', default=False)
	parser.add_argument('-l',  '--limit',     dest='limit', action='store', default=20)
	parser.add_argument('-f',  '--flag',      dest='flag', action='store_true', default=False)
	args = parser.parse_args()

	start=[]
	if args.s:
		start = load_request()

	search_id = args.search_id
	search_key = args.search_key	
	query=args.query
	limit=args.limit
	flag=args.flag
	N=args.N

	crawler = main(query, search_key, search_id, 
		N=N, start=start, limit=limit, flag=flag)