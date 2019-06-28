import generate_data

import xml.etree.cElementTree as ET
import csv
from datetime import datetime
import threading
import multiprocessing
import logging

def xml_read():
    '''
Функция читает данные с xml файла и записывает результат в список data
    '''
    root = ET.parse('example_data_log.xml').getroot() #указываем файл и ищем "корень"
    data=[] 
    for child in root:#осуществляем перебор всех объектов
        nested_data=[] #список для хранения строк каждого объекта
        for i in child:
            nested_data.append(i.text)#запись всех строк текущего объекта child
        data.append(nested_data)#запись набора строк каждого объекта в список
    return data


#print(data)

def csv_reader():
    '''
Функция читает данные с csv файла и записывает результат в csv_data
    '''
    csv_data=[]
    csv_path = "example_data_log.csv"
    with open(csv_path, "r") as file:#открытие файла с данными
        reader = csv.reader(file)
   # print (reader[0].text)
    for row in reader: #перебор всех объектов файла
        nested_csv_data=[]
        for i in row: #перебор каждой строки текущего объекта row
            nested_csv_data.append(i)
        csv_data.append(nested_csv_data)#запись набора строк каждого объекта в список
    return csv_data



class Data:
    '''
	Класс для выполнения аггрегаций
    '''
    def __init__(self):
        self.list=xml_read()#загрузка данных
        self._lock = threading.Lock()#инициализация блокировок доступа для потоков
        self.aggregations={'count':self.count, 'sum':self.summ,
                           'avg':self.avg,'max':self.max_, 'min':self.min_}# словарь функций-методов для вызова с помощью текстовой переменной
        self.results={'count':{}, 'sum':{}, 'avg':{},'max':{}, 'min':{}}# словарь результатов для каждой фунции-метода
        self.headers=['start_page', 'user', 'ts', 'depth', 'duration',
                      'transmit', 'type']
    def count(self, data, coloumn,name, col_2=None):
        """ Функция для подсчета количества элементов относительно выбранной
		колонки coloumn.

	Аргументы:data - данные, с которыми нужно проводить расчет,
	coloumn - колонка, относительно которой проводить расчет,
	name - название(номер) текущего потока.
		  
	Например, можно посчитать сколько элементов содержится у каждого типа types
		"""
        logging.info("Thread %s: starting count, timenow = %s", name, str(datetime.now())) # лог текущего потока, начинающего выполнение функции
        with self._lock: # блокировка доступа к словарю воизбежание гонки данных
            for i in data:
                if self.results['count'].get(i[coloumn]) == None: 
                    self.results['count'][i[coloumn]]=1#если словарь с результатами пустой, присваиваем текущему полю 1
                else:
                    self.results['count'][i[coloumn]]+=1 # если в словаре уже есть данные, то прибавляем к результату(количеству) единицу
 #       print (res.get('4'))
        logging.info("Thread %s: finishing count, timenow = %s", name, str(datetime.now()))# завершение потока
        return self.results['count']
        
    def summ(self,data, col_1, col_2,name):
        """ суммирование данных data по полю col_2 относительно поля col_1.
			
	Например, суммирование duration относительно типов types"""
        
        logging.info("Thread %s: starting summ, timenow = %s", name, str(datetime.now()))
        with self._lock: #блокировка доступа воизбежание гонки данных
            for i in data:
                if self.results['sum'].get(i[col_1]) == None: 
                    self.results['sum'][i[col_1]]=int(i[col_2]) #если сумма относительно col_1 еще не добавлена, то добавляем текущеее значение col_2
                else:
                    self.results['sum'][i[col_1]]+=int(i[col_2])#иначе производим суммирование поля col_2 относительно col_1
        logging.info("Thread %s: finishing summ, timenow = %s", name, str(datetime.now()))
        return self.results['sum']

    def avg(self,data, col_1, col_2,name=None):

        """ Метод для поиска среднего арифмитического.

	Аргументы: data - данные, с которыми производить расчеты,
	col_1 - поле, относительно которого производить расчеты,
	col_2 - поле, по которому производить расчеты
	
	"""
        self.results['avg']=self.summ(data,col_1,col_2,name) # вызов метода суммирования,выполняется параллельно
        self.count(data,col_1,name) #вызов метода подсчета количества, выполняется параллельно
        if name != None and name != 0: # отключаем параллельность для нижнего блока кода, оставляем поток с номером 0
            pool.shutdown()
        #print('keys = ', self.results['avg'].keys())
        for i in self.results['avg'].keys():
            try:
                self.results['avg'][i]=self.results['avg'][i]/self.results['count'].get(i) #рассчет среднего арифмитического
            except:
                logging.info("Error. Error time = %s", name, str(datetime.now()))
       # print(self.results['summ'])

    def max_(self, col_1, col_2):

        """ Поиск максимума.
	
	Аргументы: сol_1 - поле, у которого найден максимум. Например, можно указать type.
	То есть при нахождении максимума зафиксируется и type, у которого он найден.
	col_2 - поле, по которому искать максимум.
	
	"""
        max_val=int(self.list[0][col_2]) # присваиваем элемент нужного поля первого объекта в результирующую переменную
        for i in self.list:
            if max_val<int(i[col_2]):
                max_val=int(i[col_2])
                col_max=i[col_1]
        self.results['max']['val']=max_val 
        self.results['max']['col']=col_max 

    def min_(self, col_1, col_2):

        """Поиск минимума
	
	Аргументы: сol_1 - поле, у которого найден минимум. Например, можно указать type.
	То есть при нахождении минимума зафиксируется и type, у которого он найден.
	col_2 - поле, по которому искать минимум.
			   
	"""

        min_val=int(self.list[0][col_2]) # присваиваем элемент нужного поля первого объекта в результирующую переменную
        for i in self.list:
            if min_val>int(i[col_2]):
                min_val=int(i[col_2])
                col_min=i[col_1]
        self.results['min']['val']=min_val
        self.results['min']['col']=col_min

    def execute(self, aggr ,col_1, col_2=None): 

        """ Исполнение методов с использованием параллельности(кроме минимума и максимума)
	
	Аргументы: aggr - название аггрегата, который нужно выполнить
	col_1 - колонка, относительно которой выполнять вычисления
	col_2 - колонка, по которой выполнять вычисления
	"""

        if aggr=='max' or aggr == 'min':# для максимума и минимума параллельность не выполняется
            self.aggregations[aggr](col_1,col_2)
            print(self.results[aggr])
            self.generate_csv(aggr,self.results[aggr],col_1,col_2)
            self.generate_xml(aggr,self.results[aggr],col_1,col_2)
        else:
            np=4 # количество потоков
            n=len(self.list) # количество объектов
          #  print('n =  ',n)
            koef=(n//np)+1 # коэфициент
            from concurrent.futures import ThreadPoolExecutor
            startTime = datetime.now()
            self.res={}
            with ThreadPoolExecutor(max_workers=np) as pool:
                logging.info('Start parallel part of the programm')
                for ind in range(np):# выполнение параллельной части
                    pool.submit(self.aggregations[aggr], self.list[ind*koef:(ind+1)*koef], col_1,col_2,ind)  
            print(self.results[aggr])
            res=self.results[aggr]
            self.generate_csv(aggr,res,col_1,col_2)
            self.generate_xml(aggr,res,col_1,col_2)

    def generate_csv(self,agg,result,col_1,col_2):
        """ Создание csv файла с результатами """
        file='result.csv'

        with open(file, 'w') as output:
            output.write(self.headers[col_1]+',')
            output.write(str(agg)+'_'+self.headers[col_2])
            if agg == 'max' or agg == 'min':
                output.write('\n' + str(result['col']) + ',' + str(result['val']))
            else:
                for i in result:
                    output.write('\n' + str(i) + ',' + str(result[i]))


    def generate_xml(self,aggr,result,col_1,col_2):
        """ Создание csv файла с результатами """

        root = ET.Element('root')
        file='result.xml'
        if aggr == 'max' or aggr == 'min':
            row = ET.SubElement(root, 'row')
            ET.SubElement(row, self.headers[col_1]).text = str(str(result['col']))
            ET.SubElement(row, str(aggr)+'_'+self.headers[col_2]).text = str(result['val'])
        else:
            for i in result:
                row = ET.SubElement(root, 'row')
                ET.SubElement(row, self.headers[col_1]).text = str(i)
                ET.SubElement(row, str(aggr)+'_'+self.headers[col_2]).text = str(result[i])


        tree = ET.ElementTree(root)
        tree.write(file)

        with open(file, 'r') as original:
            data = original.read()
        with open(file, 'w') as modified:
            modified.write('<?xml version="1.0" encoding="UTF-8"?>\n' + data)



rows=input('Сколько требуется сгенерировать объектов данных: ')
generate_data.main(int(rows))

logging.basicConfig(level=logging.INFO)
logging.info('Start aggregation. Start time = %s', str(datetime.now()))

print('Cписок доступных аггрегатов: count, sum, avg, min, max')
agg=input('Введите аггрегат  ')

print('В наборе данных имеются следующие поля:')
for i in enumerate(Data().headers):
    print(i)
print('Напишите номер поля относительно которого вы хотите провести расчеты. Например, 6 - это type')
c_1=input('Ваш выбор = ')
print('Напишите номер поля, над которым вы хотите провести расчеты. Например, 4 - это duration')
c_2=input('Ваш выбор = ')

try:
    Data().execute(agg,int(c_1),int(c_2))
    #Data().execute(agg,6,4)
    logging.info('Done. Your result saved at result.xml and result.csv')
    logging.info('End time = %s', str(datetime.now()))  
except:
    print('Ощибка в веденных данных')
    logging.info('Error. Error time = %s', str(datetime.now()))


