# there are some packages you might need install first
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from nltk.stem.porter import PorterStemmer
import json
import io
import os
import csv

#---------------------------------------------------------------------------------------------------
# Load the json files and register it as tables; join two tables
#---------------------------------------------------------------------------------------------------
#load crew_cast.json
sqlContext = SQLContext(sc)
people_df = sqlContext.jsonFile("file:///home/hanbosun/si618/project/crew_cast.json")
people_df.printSchema()
people_df.registerTempTable("people_tb")
#load movie_info.json
movie_df = sqlContext.jsonFile("file:///home/hanbosun/si618/project/movie_info.json")
movie_df.printSchema()
movie_df.registerTempTable("movie_tb")
#join tables
tb = sqlContext.sql('''SELECT * FROM people_tb
                        JOIN movie_tb ON (people_tb.movie_id=movie_tb.id)''')

tb.registerTempTable("tb")
tb.printSchema()
#---------------------------------------------------------------------------------------------------
# Q1a - actors who appear in the most movies as leading actors, limit to top 20;
#       actors who appear in the most high rate films as leading actors, limit to top 20;
#       actors who appear in the most low rate films as leading actors, limit to top 20;
#---------------------------------------------------------------------------------------------------
#actor table
actor = sqlContext.sql('''SELECT id,actor_1_name as actor_name, vote_average FROM tb
                        UNION
                        SELECT id, actor_2_name as actor_name, vote_average FROM tb
                        UNION
                        SELECT id, actor_3_name as actor_name, vote_average FROM tb''')
actor.registerTempTable("actor")
actor.printSchema()
actor.count() #14290
# all movies
count_actor = sqlContext.sql('''SELECT actor_name, count(actor_name) as counts FROM actor
                                GROUP BY actor_name
                                ORDER BY counts DESC, actor_name LIMIT 20''')
count_actor.registerTempTable("count_actor")
count_actor.collect()
#high rate movie
count_actor_high = sqlContext.sql('''SELECT actor_name, count(actor_name) as counts FROM actor
                                WHERE vote_average >= 7
                                GROUP BY actor_name
                                ORDER BY counts DESC, actor_name LIMIT 20''')
count_actor_high.registerTempTable("count_actor_high")
count_actor_high.collect()
#low rate movie
count_actor_low = sqlContext.sql('''SELECT actor_name, count(actor_name) as counts FROM actor
                                WHERE vote_average < 7
                                GROUP BY actor_name
                                ORDER BY counts DESC, actor_name LIMIT 20''')
count_actor_low.registerTempTable("count_actor_low")
count_actor_low.collect()

c1 = count_actor.collect()
c2 = count_actor_high.collect()
c3 = count_actor_low.collect()

if os.path.exists('output_productive_actors.csv'):
    os.remove('output_productive_actors.csv')

with open('output_productive_actors.csv', 'a') as outcsv:
    writer = csv.writer(outcsv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    writer.writerow(['Actor Name', 'Production', 'Actor Name', 'High Rate Production','Actor Name', 'Low Rate Production'])
    for i in range(0,len(c1)):
        writer.writerow([str(c1[i][0]), str(c1[i][1]),str(c2[i][0]),str(c2[i][1]),str(c3[i][0]),str(c3[i][1])])


#---------------------------------------------------------------------------------------------------
# Q1b - the most productive directors, limit to top 10; the highest rate directors, top 10
#---------------------------------------------------------------------------------------------------
# production
director_prod = sqlContext.sql('''SELECT director_name, count(director_name) as counts FROM tb
                            GROUP BY director_name
                            ORDER BY counts DESC, director_name LIMIT 10''')

director_prod.printSchema()
director_prod.collect()
# quality -  at least 3 products
director_qual = sqlContext.sql('''SELECT director_name, AVG(vote_average) as rate, count(director_name) as counts FROM tb
                            GROUP BY director_name HAVING (count(director_name)>=5)
                            ORDER BY rate DESC, director_name LIMIT 10''')
director_qual.printSchema()
director_qual.collect()

c1 = director_prod.collect()
c2 = director_qual.collect()
if os.path.exists('output_directors.csv'):
    os.remove('output_directors.csv')

with open('output_directors.csv', 'a') as outcsv:
    writer = csv.writer(outcsv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    writer.writerow(['Director Name', 'Production', 'Director Name', 'Average Rate'])
    for i in range(0,len(c1)):
        writer.writerow([str(c1[i][0]), str(c1[i][1]),str(c2[i][0]),str(c2[i][1]),str(c2[i][2])])

#---------------------------------------------------------------------------------------------------
# Q1c -  co-op information
#---------------------------------------------------------------------------------------------------
actor_director = sqlContext.sql('''SELECT id,actor_1_name as actor_name, director_name FROM tb
                        UNION
                        SELECT id, actor_2_name as actor_name, director_name FROM tb
                        UNION
                        SELECT id, actor_3_name as actor_name, director_name FROM tb''')
actor_director.registerTempTable("actor_director")
actor_director.printSchema()
actor_director.count() #14290

coop = sqlContext.sql('''SELECT director_name, actor_name, count(actor_name) as counts FROM actor_director
                                GROUP BY actor_name,director_name
                                ORDER BY counts DESC, director_name, actor_name''')
coop.registerTempTable('coop')
coop_out = sqlContext.sql('''SELECT director_name, actor_name, counts FROM coop
                                WHERE counts >= 3
                                ORDER BY counts DESC, director_name, actor_name''')
coop_out.count() # 51
if os.path.exists('output_coop.csv'):
    os.remove('output_coop.csv')

with open('output_coop.csv', 'a') as outcsv:
    writer = csv.writer(outcsv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    writer.writerow(['Director Name', 'Actor Name', 'Counts'])
    for r in coop_out.collect():
        writer.writerow([str(r[0]), str(r[1]),str(r[2])])

#---------------------------------------------------------------------------------------------------
# Q2 -  the median of vote scores and mean run time in years, order in year
#---------------------------------------------------------------------------------------------------
year_rate_duration = sqlContext.sql('''SELECT year, AVG(vote_average) as ave_rate, AVG(duration) as ave_duration, AVG(budget) as ave_budget, count(year) as counts FROM tb
                            GROUP BY year
                            ORDER BY year''')

year_rate_duration.printSchema()
year_rate_duration.collect()
c1 = year_rate_duration.collect()
if os.path.exists('year_rate_duration_budge.csv'):
    os.remove('year_rate_duration_budge.csv')

with open('year_rate_duration_budge.csv', 'a') as outcsv:
    writer = csv.writer(outcsv, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    writer.writerow(['Year', 'Average Rate', 'Average Duration','ave_budget'])
    for i in range(1,len(c1)):
        writer.writerow([str(c1[i][0]), str(c1[i][1]),str(c1[i][2]),str(c1[i][3])])

#---------------------------------------------------------------------------------------------------
# Q3 - key words trend - words clouds $ two csv outputs
#---------------------------------------------------------------------------------------------------
#1987-2006
clouds_high = sqlContext.sql('''SELECT plot_keywords FROM tb
                                    WHERE vote_average>=7 AND year>=1987 AND year<2007''')
word_high = clouds_high.collect()
word_high = [x[0] for x in word_high]
word_high = [x.replace('|',' ') for x in word_high]
word_high = ' '.join(word_high)
if os.path.exists('high.txt'):
    os.remove('high.txt')

with open("high.txt", "w") as text_file:
    text_file.write(word_high.encode('utf8'))

with open('high.txt', 'r') as myfile:
    word_high=myfile.read()

word_list = word_high.split()
stemmer = PorterStemmer()
def stem_tokens(tokens, stemmer):
    stemmed = []
    for item in tokens:
        stemmed.append(stemmer.stem(item))
    return stemmed

word_stem = stem_tokens(word_list, stemmer)

if os.path.exists('high_stem.txt'):
    os.remove('high_stem.txt')

with open('high_stem.txt', 'w') as myfile:
    for s in word_stem:
        myfile.write(s)
        myfile.write(' ')

with open('high_stem.txt', 'r') as myfile:
    word_stem=myfile.read().split(' ')

len(word_stem) #7129

#low
clouds_low = sqlContext.sql('''SELECT plot_keywords FROM tb
                                    WHERE vote_average<7 AND year>=1987 AND year<2007''')
word_low = clouds_low.collect()
word_low = [x[0] for x in word_low]
word_low = [x.replace('|',' ') for x in word_low]
word_low = ' '.join(word_low)
if os.path.exists('low.txt'):
    os.remove('low.txt')

with open("low.txt", "w") as text_file:
    text_file.write(word_low.encode('utf8'))

with open('low.txt', 'r') as myfile:
    word_low=myfile.read()

word_list = word_low.split()
stemmer = PorterStemmer()
word_stem = stem_tokens(word_list, stemmer)
if os.path.exists('low_stem.txt'):
    os.remove('low_stem.txt')

with open('low_stem.txt', 'w') as myfile:
    for s in word_stem:
        myfile.write(s)
        myfile.write(' ')

with open('low_stem.txt', 'r') as myfile:
    word_stem=myfile.read().split(' ')

len(word_stem) #19865


##recent 10 years
#high
clouds_high = sqlContext.sql('''SELECT plot_keywords FROM tb
                                    WHERE vote_average>=7 AND year>=2007''')
word_high = clouds_high.collect()
word_high = [x[0] for x in word_high]
word_high = [x.replace('|',' ') for x in word_high]
word_high = ' '.join(word_high)
if os.path.exists('high_new.txt'):
    os.remove('high_new.txt')

with open("high_new.txt", "w") as text_file:
    text_file.write(word_high.encode('utf8'))

with open('high_new.txt', 'r') as myfile:
    word_high=myfile.read()

word_list = word_high.split()
stemmer = PorterStemmer()
def stem_tokens(tokens, stemmer):
    stemmed = []
    for item in tokens:
        stemmed.append(stemmer.stem(item))
    return stemmed

word_stem = stem_tokens(word_list, stemmer)

if os.path.exists('high_stem_new.txt'):
    os.remove('high_stem_new.txt')

with open('high_stem_new.txt', 'w') as myfile:
    for s in word_stem:
        myfile.write(s)
        myfile.write(' ')

with open('high_stem_new.txt', 'r') as myfile:
    word_stem=myfile.read().split(' ')

len(word_stem) #4879
#low
clouds_low = sqlContext.sql('''SELECT plot_keywords FROM tb
                                    WHERE vote_average<7 AND year>=2007''')
word_low = clouds_low.collect()
word_low = [x[0] for x in word_low]
word_low = [x.replace('|',' ') for x in word_low]
word_low = ' '.join(word_low)
if os.path.exists('low_new.txt'):
    os.remove('low_new.txt')

with open("low_new.txt", "w") as text_file:
    text_file.write(word_low.encode('utf8'))

with open('low_new.txt', 'r') as myfile:
    word_low=myfile.read()

word_list = word_low.split()
stemmer = PorterStemmer()
word_stem = stem_tokens(word_list, stemmer)
if os.path.exists('low_stem_new.txt'):
    os.remove('low_stem_new.txt')

with open('low_stem_new.txt', 'w') as myfile:
    for s in word_stem:
        myfile.write(s)
        myfile.write(' ')

with open('low_stem_new.txt', 'r') as myfile:
    word_stem=myfile.read().split(' ')

len(word_stem) #16601
