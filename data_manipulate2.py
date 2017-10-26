# require "data_manipulate1" executed
import numpy as np

release_time = original_format['release_date'].tolist()
year = []
for time in release_time:
    year.append(time.year)

year = np.asarray(year)

np.savetxt('year.txt',year,fmt='%4.0f')


original_format['year'] = pd.Series(year, index=original_format.index)

with open("all.json", mode='w', encoding='utf-8') as f:
    for index, row in original_format.iterrows():
        f.write(row.to_json(orient='index'))
        f.write('\n')

crew_cast = original_format[['actor_1_name','actor_2_name','actor_3_name','overview','plot_keywords','movie_title','id','tagline']]
crew_cast.columns = ['actor_1_name','actor_2_name','actor_3_name','overview','plot_keywords','movie_title','movie_id','tagline']
movie_info = original_format.drop(['actor_1_name','actor_2_name','actor_3_name','overview','plot_keywords','movie_title','tagline'],axis=1)

with open("crew_cast.json", mode='w', encoding='utf-8') as f:
    for index, row in crew_cast.iterrows():
        f.write(row.to_json(orient='index'))
        f.write('\n')


with open("movie_info.json", mode='w', encoding='utf-8') as f:
    for index, row in movie_info.iterrows():
        f.write(row.to_json(orient='index'))
        f.write('\n')
