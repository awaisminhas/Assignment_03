                      

PREPROCESSING

we have done the preprocessing and gave the chunk size of 100. you can adjust the size according to your need.

.

BATCH-PROCESSING


















now we have to store the data in a single json file and than do the next step.
We do batch preprocessing on small data we can just the size of data according to the need.


PIPELINING 
We have to make 1 producer and 3 consumer files in which we gave the data of batch preprocessing which gives us the real time data which now than goes to the consumers files and do their jobs according to the need.


MINNING
we make a Producer file and 3 Comsumers files 

1-Apriori
1st we have to find the combinations of itemsets than filteration on itemset in which we gave the threshold 
than apply algorithm : .json file read than frequent itemsets seperate 

2-PCY
1st we create the itemset than hash buckets → hash value calculate and increment in the hash buckets and the buckets depends on the input data what we use (like depends on the itemsets)
than we do filtering process 
than PCY approch : frequent itemset dictionary and returns the dictionary(empty )
dictionary is basically we make an empty file in which we store the frequent itemset

3-FP-growth
In this 1st we make an empty list called transactions 
inside a loop in which it consumes messages from kafka than decodes it into json data 
than it appends the category field and represents the items in transaction list.

IN FP-GROWTH WE HAVE TO FIND THE FREQUENT ITEMSETS EFFICIENTLY…
AND ALSO ERROR HANDLING…...

DATABASE INTEGRATION

MongoDB

.

I don’t know why this shows me error on ubantu 



ALSO REFER TO i211377_report








