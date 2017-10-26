# TMDB-kernel
TMDB movie Analysis: Review and Future

Source codes include:

(1) data_manipulate1.py - data manipulation convert and parse “csv”(it’s complicated since some columns themselves are json format) to pandas dataframe. Also, extract and process features we are interested in to facilitate the downstream analysis. 

(2) data_manipulate2.py - save two pandas dataframes to json files.

(3) pyspark.py - main analysis code.

(4) mrjob_word.py - for Q3
please execute something like:
## for "high": python mrjob_word.py high_stem.txt -o ./output
# cat ./output/part* > output_words_high.txt
## for "low": python mrjob_word.py low_stem.txt -o ./output
# cat ./output/part* > output_words_low.txt

(5) word_cloud, python module that need to be installed: https://github.com/amueller/word_cloud
generate word cloud - commands like: 
$ wordcloud_cli.py --width 600 --height=600  --text /Users/HanBo/Desktop/project_parta_report_hanbosun/data/high_stem.txt --imagefile /Users/HanBo/Desktop/project_parta_report_hanbosun/data/wordcloud_high.png

(6) visualization.R - generate other figures.
