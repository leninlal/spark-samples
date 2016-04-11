'''
data format:
PassengerId,Survived  (survived=0 & died=1),Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
1,0,3,"Braund Mr. Owen Harris",male,22,1,0,A/5 21171,7.25,,S,
2,1,1,"Cumings Mrs. John Bradley (Florence Briggs Thayer)",female,38,1,0,PC 17599,71.2833,C85,C,
3,1,3,"Heikkinen Miss. Laina",female,26,0,0,STON/O2. 3101282,7.925,,S,
4,1,1,"Futrelle Mrs. Jacques Heath (Lily May Peel)",female,35,1,0,113803,53.1,C123,S,
5,0,3,"Allen Mr. William Henry",male,35,0,0,373450,8.05,,S,

problem:
number of people who died or survived in each class, along with their gender and age.


'''

from pyspark import SparkContext

if __name__ == '__main__':
    sc = SparkContext(appName="TitanicAnalysis")
    textfile = sc.textFile("/path/to/TitanicData.txt")
    split= textfile.filter(lambda x: len(x.split(',')) >= 6).map(lambda line: line.split(','))
    count=split.map(lambda x: (x[1]+" "+x[4]+" "+x[6]+" "+x[2],1)).reduceByKey(lambda a, b: a+b )
