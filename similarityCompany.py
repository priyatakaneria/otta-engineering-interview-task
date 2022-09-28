import pandas as pd
import duckdb
import csv

# read the reactions.csv and jobs.csv file
dfReactions = pd.read_csv('data/reactions.csv')
dfJobs = pd.read_csv('data/jobs.csv')

# create a csv file to store the similarity scores
with open('companySimilarityScore.csv', newline='', mode='w') as similarity_file:
    similarity_writer = csv.writer(similarity_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    # write the column names to the file
    header = ['company1', 'company2', 'similarity_score']
    similarity_writer.writerow(header)
    # join the reactions and jobs tables together based on job_id
    innerJoin = duckdb.query("select * from dfReactions inner join dfJobs on (dfReactions.job_id = dfJobs.job_id);").to_df()
    # query the database to remove all FALSE rows from the dataframe and order by user_ids
    trueOnly = duckdb.query("select * from innerJoin where direction=TRUE order by user_id;").to_df()
    # get all unique company IDs in order to create the company pairings for similarity scores
    companies = trueOnly.company_id.unique()

    # loop through all company pairings, avoiding pairing the same company ID to itself or repeating pairings
    for x in range(len(companies)):
        for y in range(x+1, len(companies)):
            # retrieve the TRUE job listings for both company IDs
            output1 = duckdb.query("select * from trueOnly where company_id=" + str(companies[x]) + ";").to_df()
            output2 = duckdb.query("select * from trueOnly where company_id=" + str(companies[y]) + ";").to_df()
            # take the unique user listings from both results to avoid repeats and put this into an array
            users1 = output1.user_id.unique()
            users2 = output2.user_id.unique()
            start = 0
            count = 0
            # search to find which users appear in both arrays
            # "start" is used here to begin each next search at the position it left off earlier to increase efficiency
            # given that the dataframe was ordered it will not be matched with something above what it previously found
            for i in range(len(users1)):
                for j in range(start, len(users2)):
                    if (users1[i] == users2[j]):
                        count+=1
                        start = j
                        break
            # write the score to the file
            similarity_writer.writerow([str(companies[x]), str(companies[y]), str(count)])

# read in the file and find the maximum similarity score
similaritydf = pd.read_csv('companySimilarityScore.csv')
max_val = similaritydf.loc[similaritydf["similarity_score"].idxmax()]
print(max_val)