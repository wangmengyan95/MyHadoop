MyHadoop
========
In this project, I implemented a Map-Reduce Facility similar to Hadoop, but with certain design 
constraints aimed at enabling it to work more efficiently in CMU AFS computing environment with smaller data sets. The framework is able to

1. Execute several jobs concurrently and correctly, without concurrency related problems, except as the result of programmer-visible and mitigatable sharing

2. Schedule and dispatch maps and reduces, to maximize the performance gain through parallelism within each phase

3. Recover from failure of map and reduce tasks 

4. Provide a general-purpose I/O facility to support the necessary operations

5. Provide management tools to management job and monitor status of the clusters
