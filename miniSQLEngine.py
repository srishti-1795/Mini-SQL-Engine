#!/usr/bin/python
#coding=utf-8

import csv
import sys
import re
import sqlparse
import os
import numbers

metadataDictionary = {}

def main():
	readMetadata()
	print metadataDictionary

	while(1):
		print "msql>",
		query = raw_input()
		if(query == "exit" or query == "quit"):
			break;
		else:
			process(query)

def readMetadata():
	f = open('sampleData/metadata.txt','r')
	check = 0
	for line in f:
		if line.strip() == "<begin_table>":
			check = 1
			continue
		if check == 1:
			tableName = line.strip()
			metadataDictionary[tableName] = [];
			check = 0
			continue
		if not line.strip() == '<end_table>':
			metadataDictionary[tableName].append(line.strip());	

	for i in metadataDictionary:
		metadataDictionary[i] = filter(None, metadataDictionary[i])
	
def process(query):
	parseQuery(query)


def parseQuery(query):
	try:
		parsedQuery = sqlparse.parse(query)[0].tokens
		queryType = sqlparse.sql.Statement(parsedQuery).get_type()
		identifierList = []
		l = sqlparse.sql.IdentifierList(parsedQuery).get_identifiers()
		
		for i in l:
			identifierList.append(str(i))
		readMetadata()

		if (queryType == 'SELECT'):
			processSelect(parsedQuery, identifierList)
		elif (queryType == 'CREATE'):
			processCreate(parsedQuery, identifierList)
		elif (queryType == 'INSERT'):
			processInsert(parsedQuery, identifierList)
		elif (queryType == 'DELETE'):
			processDelete(parsedQuery, identifierList)
		#elif (queryType == 'TRUNCATE'):
		#	processTruncate(parsedQuery, identifierList)
		elif (queryType == 'DROP'):
			processDrop(parsedQuery, identifierList)	
		else:
			if(identifierList[0] == 'TRUNCATE'):
				processTruncate(parsedQuery, identifierList)
			else:
				print "invalid"

	except:
		pass

def processSelect(parsedQuery, identifierList):
	if (len(identifierList) == 4):
		filePath = 'sampleData/'+ identifierList[3] + '.csv'
		function = re.sub(ur"[\(\)]",' ',identifierList[1]).split()
		#print function

		if (os.path.exists(filePath)):
			f = open(filePath,'rb')
			reader = csv.reader(f)

			#select * from <table>
			#printing the entire table
			if (function[0] == '*'):
				for i in metadataDictionary[identifierList[3]]:
					print i+'\t',
				print '\n'

				for row in reader:
					for i in row:
						print i+'\t',
					print '\n'

			#aggregate functions : min, max, avg, sum
			#distinct projections
			elif(function[0] == 'max' or function[0] == 'min' or function[0] == 'sum' or function[0] == 'avg' or function[0] =='distinct'):
				attribute = function[1]
				m = open('Data/metadata.txt')
				columnNumber = 0
				flag =0
				found = 0
				for line in m:
					if (line.strip() == identifierList[3]):
						flag =1
					if(flag == 1):
						if(line.strip() == '<end_table>'):
							#checking if attribute exists
							if(found != 1):
								print "ERROR: attribute does not exist"
								return
						if(line.strip()!=attribute):
							columnNumber += 1
						if(line.strip() == attribute):
							found = 1;
							break
				#print count
				#print columnNumber
				m.close()
				values=[]
				for row in reader:
					values.append(int(row[columnNumber-1]))

				print identifierList[1]
				if(function[0] == 'max'):
					print max(values)
				elif(function[0] == 'min'):
					print min(values)
				elif(function[0] == 'sum'):
					print reduce(lambda x,y:x+y,values)
				elif(function[0] == 'avg'):
					print reduce(lambda x,y:x+y,values)/float(len(values))
				elif(function[0] == 'distinct'):
					newValues = list(set(values))
					for i in newValues:
						print i
				else:
					pass

			else:
				attributes = re.sub(ur"[\,]",' ',identifierList[1]).split()
				#print attributes
				tables = re.sub(ur"[\,]",' ',identifierList[3]).split()
				#print tables

				#for a single table
				if (len(tables)==1):
					table = tables[0]
					columnNumbers = []
					for i in attributes:
						print i+'\t',
						if i in metadataDictionary[table]:
							columnNumbers.append(metadataDictionary[table].index(i))
						else:
							print"ERROR: attribute doesn't exist"
							return
					print '\n'
					for row in reader:
						for i in columnNumbers:
							print row[i]+'\t',
						print '\n'
				# else:
				# 	columnNumbers=[]
				# 	d={}
				# 	for i in attributes:
				# 		count = 0;
				# 		for j in tables:
				# 			if i in metadataDictionary[j]:
				# 				d[i] = []
				# 				d[i].append(j,metadataDictionary[j].index(i))
				# 				count+=1
				# 			if (count > 1):
				# 				print "ERROR:ambiguous attribute"
				# 				return
				# 	print d
			f.close()
			return
		else:
			print "ERROR: table does not exist"
	
	elif(len(identifierList) == 5):
		# attributes = re.sub(ur"[\,]",' ',identifierList[1]).split()
		# tables = re.sub(ur"[\,]",' ',identifierList[3]).split()
		# where = identifierList[4].split()
		# filePath = 'Data/'+ identifierList[3] + '.csv'
		# f = open(filePath,'rb')
		# reader = csv.reader(f)
		# print where
		# #selecting from a single table
		# if(len(tables)==1):
		# 	table=tables[0]
		# 	if (len(where)==2):
		# 		args = re.sub(ur"[\=]",' ',where[-1])
		# 		for i in attributes:
		# 			print i+'\t',
		# 				if i in metadataDictionary[table]:
		# 					columnNumbers.append(metadataDictionary[table].index(i))
		# 				else:
		# 					print"ERROR: attribute doesn't exist"
		# 					return
		# 			print '\n'

		# 		for row in reader:
		# 				for i in columnNumbers:
		# 					if(row[i])
		# 					print row[i]+'\t',
		# 				print '\n'

		# 	elif(len(where)==4):
		# 		if 'AND' in where:
		# 			pass
		# 		elif 'OR' in where:
		# 			pass
		# 	else:
		# 		print "ERROR: invlaid where"

		# #multiple tables
		# else:
		# 	pass
		# f.close()

		pass	
	else:
		print"ERROR:invalid"
	
def processCreate(parsedQuery, identifierList):
	#print identifierList
	if (identifierList[1] != 'TABLE'):
		print "ERROR: Invalid query"
		return

	info = identifierList[2]
	info = re.sub(ur"[\(\)\,]",' ',info)
	infoList = filter(None,info.split(' '))
	#print infoList
	if(len(infoList)<3):
		print "ERROR: Invalid"

	filePath = 'Data/'+infoList[0]+'.csv'
	if os.path.exists(filePath):
		#if table already exists: ERROR
		print "ERROR: Table already exists!"
	else:
		#creating new csv file
		f = open(filePath,'w')
		f.close()
		
		#modifying metadata.txt
		f = open('Data/metadata.txt','a')
		f.write('\n<begin_table>\n')
		f.write(infoList[0]+'\n')
		i = 1;
		while (i<len(infoList)):
			f.write(infoList[i] + '\n')
			i +=2
		f.write('<end_table>')		
		f.close()
	
def processInsert(parsedQuery, identifierList):
	if (identifierList[1] != "INTO" or identifierList[3]!='VALUES'):
		print ("ERROR: invalid query")
		return

	filePath = 'Data/'+identifierList[2]+'.csv'
	#checking number of values and number of attributes are same
	values = re.sub(ur"[\(\)\,]",' ',identifierList[4]).split()
	
	for i in values:
		#checking to see that all are number sonly
		if not i.isdigit() and not i[1:].isdigit():
			print "ERROR: only integer values allowed"
			return

	if os.path.exists(filePath):
		if (len(values) != len(metadataDictionary[identifierList[2]])):
			print "ERROR! invalid query"
			return
	
		c = csv.writer(open(filePath,'a'))
		c.writerow(values)

	else:
		print "ERROR: table does not exist"
		return

def processDelete(parsedQuery, identifierList):
	if (identifierList[1]!='FROM' or identifierList[3].split()[0].upper()!='WHERE'):
		print "ERROR: invalid query"
		return
	table = identifierList[2]
	filePath = 'Data/' + identifierList[2] + '.csv'
	if os.path.exists(filePath):
		l =re.sub(ur"[\=]",' = ',identifierList[3]).split()
		attribute = l[1]
		value = l[3]
		#finding attribute in metadata.txt, to find column number
		f = open('Data/metadata.txt')
		count = 0
		flag =0
		found = 0
		for line in f:
			if (line.strip() == table):
				flag =1
			if(flag == 1):
				if(line.strip() == '<end_table>'):
					#checking if attribute exists
					if(found != 1):
						print "ERROR: attribute does not exist"
						return
				if(line.strip()!=attribute):
					count += 1
				if(line.strip() == attribute):
					found = 1;
					break
		
		#print count
		f.close()

		#finding the entry in csv file, and deleting
		f=open(filePath,'rb')
		reader = csv.reader(f)
		lineNumber = 0
		data=[]
		for row in reader:
			lineNumber += 1
			if (row[count-1] == value):
				pass
			else:
				data.append(row)

		f.close()

		f=open(filePath,'wb')
		c = csv.writer(f)
		c.writerows(data)
		f.close()
		
	else:
		print "ERROR: table does not exist"
		return;

def processTruncate(parsedQuery, identifierList):
	if (identifierList[1] != 'TABLE'):
		print "ERROR: Invalid query"
		return

	filePath = 'Data/' + identifierList[2] +'.csv'
	if os.path.exists(filePath):
		#deleting all content of the csv file
		f = open(filePath,'w+')
		f.close
	else:
		print "ERROR: table does not exist."
	
def processDrop(parsedQuery, identifierList):
	if (identifierList[1] != 'TABLE'):
		print "ERROR: Invalid query"
		return

	filePath = 'Data/' + identifierList[2] +'.csv'
	if os.path.exists(filePath):
		if(os.stat(filePath).st_size == 0):
			#removing from metadata.txt
			f = open('Data/metadata.txt')
			lines =[]
			flag = 0
			for line in f:
				if (line .strip() == identifierList[2]):
					lines.pop()
					flag = 1
				else:
					if (flag == 1):
						if (line == '<end_table>'):
							flag = 0
					else:
						lines.append(line)
			f.close()
			f = open('Data/metadata.txt','w')
			f.writelines(lines)
			f.close()

			#removing csv file
			os.remove(filePath)
		else:
			print "ERROR: not an empty table."
	else:
		print "ERROR: table does not exist."
	

if __name__ == "__main__":
	main()
	