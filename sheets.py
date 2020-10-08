#google sheets api libraries
import gspread
from oauth2client.service_account import ServiceAccountCredentials
#hdx api libraries
from hdx.utilities.easy_logging import setup_logging
from hdx.hdx_configuration import Configuration
from hdx.data.dataset import Dataset
#other libraries
from pprint import pprint
import os
import pandas

#Hooking up to sheets:
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
client = gspread.authorize(creds)

spreadsheet=client.open("Coronavirus en Costa Rica")
sheet=spreadsheet.worksheet("Estado Actual")
projectionSheet=spreadsheet.worksheet("Datos Extra")


#HDX API
setup_logging()
Configuration.create(hdx_site='prod', user_agent='zkwinkle', hdx_read_only=True)

dataset = Dataset.read_from_hdx('novel-coronavirus-2019-ncov-cases')#import dataset

resources = Dataset.get_resources(dataset)[:3]#import resource objects



def erase_files(directory):
    for file in os.listdir(directory):
        path=os.path.join(directory,file)
        os.remove(path)
        print('File deleted:'+file)
    else:
        os.rmdir(directory)
        print('Folder deleted:'+directory)


directory = os.getcwd()+'\csv files'
if os.path.exists(directory):
    erase_files(directory)
os.makedirs("csv files")
print('Folder created:'+directory)


names=[]
print(directory)
for resource in resources: #downloading of csv files
    url, path = resource.download(directory)
    print('Resource URL %s downloaded to %s' % (url, path))


print(os.listdir(directory))
data=[]#[0]=confirmed, [1]=deaths, [2]=recovered
for file in os.listdir(directory): #opening csv files to dictionaries
    path=os.path.join(directory,file)
    df = pandas.read_csv(path,index_col=1)
    print('File read: %s'%file)
    data.append(df.loc['Costa Rica'].to_dict())#dictionaries get put in data list: [0]=confirmed, [1]=deaths, [2]=recovered

#Get dates nice in a list
dates=list(data[0].keys())[3:]
dates=dates[dates.index('3/6/20'):] #6/3/20 is CR's first case

erase_files(directory)

##############data in list data: [0]=confirmed, [1]=deaths, [2]=recovered
##############now information gets formatted for google sheets    
def format_line(rowIndex):

    i=rowIndex-2
    
    date=dates[-(i+1)]
    confirmed=int(data[0][date])
    deaths=int(data[1][date])
    recovered=int(data[2][date])

    newCases=("=B%d-B%d"%(rowIndex,rowIndex+1))
    activeCases=("=B%d-D%d-E%d"%(rowIndex,rowIndex,rowIndex))
    infectionRate=("=C%d/F%d")%(rowIndex,rowIndex+1)
    if rowIndex==len(dates)+1:
        infectionRate=''
    
    return [date,confirmed,newCases,deaths,recovered,activeCases,infectionRate]

def update_sheet():
    newRows=[]
    for row in range(2,len(dates)+2): #+1 porque empiezan en la fila 1 y no 0 y +1 porque empieza en la fila 2
        newRows.append(format_line(row))

    updateRange='A2:G%d'%(len(dates)+1) #+1 porque empieza en la fila 2
        
    sheet.update(updateRange,newRows,value_input_option='USER_ENTERED')
    return

def update_dataSheet():
    dates=sheet.col_values(1)
    length=len(dates)
    
    rows=[]
    reversedData=[]
    for i in range(2,length+1):
        A=("='Estado Actual'!A%d"%(i))
        B=("='Estado Actual'!F%d"%(i))
        C=("='Estado Actual'!D%d-'Estado Actual'!D%d"%(i,i+1))
        D=("='Estado Actual'!E%d-'Estado Actual'!E%d"%(i,i+1))
        E=("='Estado Actual'!G%d"%(i))
        F=("=AVERAGE('Estado Actual'!F%d:F%d)"%(i,i+7))
        G=("='Estado Actual'!D%d+'Estado Actual'!E%d"%(i,i))
        H=("=G%d-G%d"%(i,i+1))
        I=("=IF(ISBLANK('Estado Actual'!F%d),,H%d/'Estado Actual'!F%d)"%(i+1,i,i+1))
        rows.append([A,B,C,D,E,F,G,H,I])
        
        #flipped stuff
        O=("='Estado Actual'!A%d"%(length+2-i)) #fechas
        P=("='Estado Actual'!F%d"%(length+2-i)) #activos
        Q=("='Estado Actual'!E%d"%(length+2-i)) #recuperados
        R=("='Estado Actual'!D%d"%(length+2-i)) #muertos
        S=("=F%d"%(length+2-i)) #moving avg
        reversedData.append([O,P,Q,R,S])
        
    updateRange1='A2:I%d'%(length)
    updateRange2='O2:S%d'%(length)
    projectionSheet.batch_update([{'range':updateRange1,'values':rows},{'range':updateRange2,'values':reversedData}],value_input_option='USER_ENTERED')
    
    if (len(projectionSheet.col_values(1))>len(dates)):
        values_clear('A%d:H%d'%(length+1,len(projectionSheet.col_values(1))))
        values_clear('N%d:R%d'%(length+1,len(projectionSheet.col_values(1))))


update_sheet()
update_dataSheet()


print("Updated! Last date added: "+sheet.acell('A2').value)
print("\nPress enter to exit...")
input()
