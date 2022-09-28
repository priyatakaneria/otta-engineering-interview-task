import pandas as pd
import duckdb
import csv

# read the reactions.csv file
df = pd.read_csv('data/reactions.csv')

# create a csv file to store the similarity scores
with open('similarityScore.csv', newline='', mode='w') as similarity_file:
    similarity_writer = csv.writer(similarity_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    # write the column names to the file
    header = ['user1', 'user2', 'similarity_score']
    similarity_writer.writerow(header)
    # query the database to remove all FALSE rows from the dataframe, and remove all jobs which only appear once as there won't be any similarity scores generated from these
    trueOnly = duckdb.query("select *, count(*) over (partition by job_id) as Count from df where direction=TRUE order by job_id;").to_df()
    removeSingles = duckdb.query("select * from trueOnly where Count > 1;").to_df()
    # get all unique user IDs in order to create the user pairings for similarity scores
    users = removeSingles.user_id.unique()

    # loop through all user pairings, avoiding pairing the same user ID to itself or repeating pairings
    for x in range(len(users)):
        for y in range(x+1, len(users)):
            # retrieve the TRUE job listings for both user IDs
            output1 = duckdb.query("select * from removeSingles where user_id=" + str(users[x]) + ";").to_df()
            output2 = duckdb.query("select * from removeSingles where user_id=" + str(users[y]) + ";").to_df()
            # take the unique job listings to avoid repeats and put this into an array
            jobs1 = output1.job_id.unique()
            jobs2 = output2.job_id.unique()
            start = 0
            count = 0
            # search to find which jobs appear in both arrays
            # "start" is used here to begin each next search at the position it left off earlier to increase efficiency
            # given that the dataframe was ordered it will not be matched with something above what it previously found
            for i in range(len(jobs1)):
                for j in range(start, len(jobs2)):
                    if (jobs1[i] == jobs2[j]):
                        count+=1
                        start = j
                        break
            # write the score to the file
            similarity_writer.writerow([str(users[x]), str(users[y]), str(count)])

# read in the file and find the maximum similarity score
similaritydf = pd.read_csv('similarityScore.csv')
max_val = similaritydf.loc[similaritydf["similarity_score"].idxmax()]
print(max_val)