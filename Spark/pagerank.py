#Vamsy Ram Kundula
#vkundula@uncc.edu
import re
import sys
from operator import add
from pyspark import SparkContext,SparkConf

#function to seperate the tab spaced values
def parseLinks(urls):
    parts = re.split(r'\t', urls)
    return parts[0], parts[1]
#function to parse title and the links
def ParseTitleAndText(line):
	match = re.match(r'.*<title>(.*?)</title>',line)
	title = match.group(1)
	data = []
	if re.match(r'.*<text(.*?)</text>',line) is not None:
		text = re.match(r'.*<text(.*?)</text>',line).group(1)
		links = re.findall(r'\[\[(.*?)\]\]',text)
		
		for link in links:
			data.append(title+"\t"+link) 
			
	return data
	
def calculate_rank(links,pagerank):
	links_length = len(links)
	for link in links:
		yield(link, pagerank/links_length)
	
	
if __name__ == "__main__":
	if len(sys.argv) != 3:
	        print >> sys.stderr, "Command Format: spark-submit pagerank.py <input_directory> <output_directory>"
	        exit(-1)
	#get the input and output directories from the command line arguments		
	inputDir = str(sys.argv[1])
	outputDir = str(sys.argv[2])
										   
	sc = SparkContext(appName="PageRank")
	#lines RDD to read the input text file 
	lines = sc.textFile(inputDir)
	#Filter the empty and lines
	lines = lines.filter(lambda x: x!="")
	filteredLines = lines.filter(lambda x: not re.match(r'^\s*$', x))
	#initial page rank based on number of lines in the input text file
	initialPageRank = 1.0/lines.count()
	#seperates the title and the links
	links = filteredLines.flatMap(lambda line: ParseTitleAndText(line))
	seperateLinks= links.map(lambda x: parseLinks(x)) 
	links = seperateLinks.groupByKey()
	links.collect()
	pagerank = links.map(lambda page: (page[0], initialPageRank))
	links.join(pagerank) 
	#loop to iterate the page rank calculation for 10 times
	for x in range(0, 10):
		rank = links.join(pagerank).flatMap(lambda x : calculate_rank(x[1][0],x[1][1]))
		#calculate the page rank
		pagerank = rank.reduceByKey(lambda x,y: 0.15+(x+y)*0.85)
	#sort the pages with descending pagerank value and print page TAB pagerank
	pagerank = pagerank.sortBy(lambda x: x[1], ascending=False, numPartitions=None).saveAsTextFile(sys.argv[2])
	sc.stop()
