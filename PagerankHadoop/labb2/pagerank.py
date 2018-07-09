
from pyspark import SparkContext

import re
import sys
from operator import add

reload(sys)
sys.setdefaultencoding('utf-8')


def computeContributions(urls, rank):
    #Calculates URL contributions to the rank of other URLs.
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parsePages(page):
	title = re.findall(r'<title>(.*?)</title>', page)
	outlinks = re.findall(r'\[\[([^]]*)\]\]', page)
	return title[0],outlinks

File = "wiki-micro.txt"  # Should be some file on your system

# Initialize the spark context.
sc = SparkContext("local","PythonPageRank")


lines = sc.textFile(File)

count = lines.count()
print(count)
links = lines.map(lambda e: parsePages(e))

# Loads all URLs from input file and initialize their neighbors.
#links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
# Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
ranks = links.map(lambda link: (link[0], 1/float(count)))

# Calculates and updates URL ranks continuously using PageRank algorithm.
for iteration in range(2):
	# Calculates URL contributions to the rank of other URLs.
	# print(links.join(ranks).collect())
	contributions = links.join(ranks).flatMap(
		lambda (url, (urls, rank)): computeContributions(urls, rank))
	#contributions1 = sc.union([ranks.subtractByKey(contributions),contributions])
	#print(contributions1.collect())
	# Re-calculates URL ranks based on neighbor contributions.
	ranks = contributions.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

# Collects all URL ranks and dump them to console.
for (link, rank) in ranks.collect():
	print "%s has rank: %s." % (link, rank)
       
	
#sc.stop()
