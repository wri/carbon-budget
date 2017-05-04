import multiprocessing
import subprocess
import utilities

year_win_li = []
for window in range(1, 24):
    
    if window <10:
        window = "0{}".format(window)
        
    window = str(window)
    
    winpath = "Win{}".format(window)
    
    for year in range (2000, 2018):
        year_window_dict = {'year':year, 'winpath':winpath}
        year_win_li.append(year_window_dict)
print year_win_li

['winpath': 'Win23', 'year': 2015}, {'winpath': 'Win23', 'year': 2016}]
if __name__ == '__main__':
     count = multiprocessing.cpu_count()
     pool = multiprocessing.Pool(processes=1)
     pool.map(utilities.download_burned_area, year_win_li)

