# Apriori-Algorithim

University students have a great deal of freedom in deciding the order in which to take their courses. In this project we apply the Apriori-based Generalized Sequential Pattern (GSP) algorithm to undergraduate course data from a large university in order to identify frequent course sequences. 

We used the Apriori-based Generalized Sequential Pattern (GSP) algorithm to identify frequent k-sequences, where a k‑sequence contains k courses taken in a specific sequential order. We apply this method to courses at the department level and in some cases across departments. 

The GSP algorithm is a modest extension of the Apriori algorithm. The extension involves making the Apriori algorithm sensitive to the order of items (i.e., courses) in each transaction. There are two extensions that need to be made to the Apriori algorithm. The first is to extend the candidate generation phase to generate ordered itemsets and the second is to account for item ordering when computing the support for an itemset (i.e., sequence).
