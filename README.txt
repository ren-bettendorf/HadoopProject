List of the files used:

My files were broken down into three folders. The two containing the jobs are HousingJobs and SocialJobs, while the RecordUtil are where all the Records are packaged. Inside of the 
HousingJobs and SocialJobs are the actual MapReduce sections with each job, mapper, reducer, and combiner into their own folder.

------------

Other important information:

Starting a MapReduce job can be completed by: $HADOOP/bin/hadoop jar dist/censusdata.jar <path-to-job> <input-directory> <output-directory>
Viewing a MapReduce job can be completed by: $HADOOP/bin/hdfs dfs -cat <path-to-output-directory>/part-r-00000

------------

The following is where each job is located
Q1: HousingJobs/RentedOwned/RentedOwnedJob
Q2: SocialJobs/NonMarried/NonMarriedJob
Q3: SocialJobs/Hispanic/HispanicJob
Q4: HousingJobs/RuralUrban/HousingRuralUrbanJob
Q5: HousingJobs/MedianValue/HouseMedianValueJob
Q6: HousingJobs/MedianRent/RentMedianJob
Q7: HousingJobs/AverageRooms/AverageRoomJob
Q8: SocialJobs/ElderlyPopulation/ElderlyJob
Q9: SocialJobs/Comparison/ComparisonJob

If you have any questions you can reach me via phone at (707) 599-1696 or email at renaldorini@gmail.com