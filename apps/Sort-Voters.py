import time
import requests
import pandas as pd 


voterDataframe = pd.read_csv(r"/home/ElectionProject/inbound/Voter-Data-inbound.csv",encoding='utf-8-sig')
print(voterDataframe.index)
voterDataframe['ROW_NUMBER'] = voterDataframe.reset_index().index
voterDataframe.to_csv (r'/home/ElectionProject/in-progress/ArrangedVoters.csv', index = False, header=True)