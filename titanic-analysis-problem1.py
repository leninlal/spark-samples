'''
data format:
PassengerId,Survived  (survived=0 & died=1),Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
1,0,3,"Braund Mr. Owen Harris",male,22,1,0,A/5 21171,7.25,,S,
2,1,1,"Cumings Mrs. John Bradley (Florence Briggs Thayer)",female,38,1,0,PC 17599,71.2833,C85,C,
3,1,3,"Heikkinen Miss. Laina",female,26,0,0,STON/O2. 3101282,7.925,,S,
4,1,1,"Futrelle Mrs. Jacques Heath (Lily May Peel)",female,35,1,0,113803,53.1,C123,S,
5,0,3,"Allen Mr. William Henry",male,35,0,0,373450,8.05,,S,

problem:
average age of males and females who died in the Titanic tragedy


'''

from pyspark import SparkContext

def add_values(x,y):
  return (x[0]+y[0],x[1]+y[1])

if __name__ == '__main__':
    sc = SparkContext(appName="TitanicAnalysis")
    textfile = sc.textFile("/path/to/TitanicData.txt")
    split= textfile.filter(lambda x: len(x.split(',')) >= 6).map(lambda line: line.split(','))
    key_value = split.filter(lambda x: x[1]=="1" and x[5].isdigit()).map(lambda x: (x[4],int(x[5])))
    key_value.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1])).mapValues(lambda x: float(x[0])/float(x[1])).collectAsMap()
    sc.stop()
