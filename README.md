
<p align="center">
  <img src="https://github.com/dssg/El_Salvador_mined_education/blob/master/images/dsapplogo2017small-1.png">
</p>

# Reducing Early School Dropout Rates in El Salvador

To view the code produced for this project by the 2018 Data Science for Social Good (DSSG) fellows, click [here](https://github.com/dssg/El_Salvador_mined_education/tree/dssg_2018).

## The Policy Problem:

Each year from 2010 through 2016, 15-29% of students enrolled in school in El Salvador did not return to school in the following year. This high dropout rate is cause for serious concern; students dropping out of school have significant consequences for economic productivity, workforce skill, inclusiveness of growth, social cohesion, and increasing youth risks. El Salvador's Ministry of Education has an annual budget for social services to improve school performance and reduce student dropout and programs to reduce gang and drug related violence. However, the budget for these programs is not large enough to target every student and school in El Salvador. Therefore, it is critical that the Ministry of Education have an early warning system that identifies students at high risk of dropping out of school so that they can target limited resources where they will have the greatest impact on reducing dropout rates.

## The Machine Learning Problem: 

### Objective
The objective of this research is to build a model that accurately predicts the risk that a student in El Salvador will drop out of school the following year. We assign a student a label of 1 (dropout = True) in a given year if they attended school in that year, but did not attend school in the following year. The exception is if the student is in the final year of their education (based upon their bachillerato type), in which case they are assigned a label of 0 (dropout = False). We want to train a model that successfully assigns students with labels of 1 higher risk scores and assigns students with labels of 0 lower risk scores. We also want to identify which variables are the most important to predict dropout risk. 

### Unit of Analysis + Temporal Cross-Validation
Predictions are made at the year-student level (eg. as of January 1, 2009, what is the risk that a given student will not return to school in 2010?). We used data from 2009-2018 for this analysis. While we were provided some data from 2008, most of the data was missing so we excluded that year from our analysis.  We used a temporal cross-validation approach to train the models to ensure that our final model will generalize effectively to new data. As illustrated by Figure 1 below, this temporal cross-validation approach splits the data by time - training each individual model on increasing amounts of data 

![alt text](https://github.com/dssg/El_Salvador_mined_education/blob/master/images/temporalcrossvalidation.png "Temporal Cross-Validation")

For each model, we define the cohort for our prediction as all students who are enrolled in school on the “as-of-date”. Therefore, if a student is “dropped out” on January 01, 2014, they will not be included in the cohort for the January 01, 2014 prediction date and we will not predict whether they will remain “dropped out” in 2015. 

### Feature Generation

We generated several features capturing dimensions of the student and school that could help predict a student's risk of dropping out of school. The feature groups created (with illustrative features) are given below:
- **Overage:** whether/by how much student is older than the average age in their grade
- **Repeat:** whether/how many times student has repeated a grade
- **Rural:** whether student attends school in a rural area
- **Events:** number of different schools a student attended in a given year (based upon enrollment events)
- **Dropouts:** number of previous times a student dropped out of school
- **Commute:** transportation method used by student to commute to school
- **Illness:** whether student had a recorded illness 
- **Family:** number of family members, whether father is present, etc.
- **Violence:** dropout due to presence of gangs, dropout due to work, drug factors, sexual exploitation, etc.
- **School:** number of classrooms, number of student computers, electricity source, etc.
- **Department/Municipality:** whether the student is located in each Municipality and Department of El Salvador

Each of the above features are aggregated over different periods of time (e.g. in the past 1 year, past 3 years, past 5 years, etc.) using different aggregations (e.g. average, maximum value, sum). We did not include the violence, school, and department/municipality features in the initial set of models trained in the configuration file below due to time and memory constraints. We would recommend including these features in future analysis. 

## Triage Configuration File

The modeling experiments for this project were run using the DSaPP [Triage](https://github.com/dssg/triage) module. The configuration files used for this project can all be found [here](https://github.com/dssg/El_Salvador_mined_education/tree/master/experiments). Please reference the [readme_config.yaml](https://github.com/dssg/El_Salvador_mined_education/blob/master/experiments/readme_config.yaml) file for the set of configuration parameters that build all of the final models discussed in this document. 

## Results

We selected the best random forest, decision tree, and scaled logistic regression models<sup>2</sup> from all the models trained based upon the following two metrics:
- Best average precision at 10% across all years
- Best average recall at 10% across all years

We compared our results of our three models against two baselines: 1) decision-rule baseline and 2) most frequent baseline. For the decision-rule baseline, we used a decision rule that predicted students would drop out in the following year if all three of the following conditions (identified as important by MINED) were met in a given year (and would not drop out otherwise):
- overage: student's age is 2 standard deviations above the average age in their grade
- repeater: student has repeated a grade since their last enrollment in school<sup>1</sup>
- rural: whether the student attends a school in a rural area

Figure 2 below shows the performance of our top three model groups on the metric of precision at 10% and figure 3 shows performance on the metric of recall at 10% against both of the baselines. These plots illuminate a few key findings: 1) all of the models provide considerable prediction gains over the random baseline and the decision-rule baseline. We see that the precision at 10% is nearly 20% above the baselines in each year. Second, we see that the performance of all of the models is fairly consistent over time. The random forest model is the most consistent over time while the scaled logistic regression and decision tree peak in performance in 2012, the year with the highest dropout rate in our time series.

### Figure 2: Precision at 10% by Model Group and Year

![alt text](https://github.com/dssg/El_Salvador_mined_education/blob/master/images/precision_10.png "Precision at 10%")

*Figure 2 above shows the performance of each of our model groups on the metric of precision at 10% across the 6 prediction dates used in temporal cross-validation. Precision at 10% measures the ratio of true positives (students predicted to drop out by the model that actually drop out) over the number of students in 10% of the population. This tells us how precisely our model identifies true dropouts if we predict that all students in the top 10% of risk scores will drop out.*

### Figure 3: Recall at 10% by Model Group and Year

![alt text](https://github.com/dssg/El_Salvador_mined_education/blob/master/images/recall_10.png "Recall at 10%")

*Figure 3 above shows the performance of each of our model groups on the metric of recall at 10% across the 6 prediction dates used in temporal cross-validation. Recall at 10% measures the percentage of all true dropouts in our cohort identified by our model if we predict that all students in the top 10% of risk scores will drop out. The most frequent baseline assigns all observations a label of 0 as not dropping out of school (label = 0) is more common than dropping out of school (label = 1). Therefore, to calculate recall at 10% for the most frequent baseline, we randomly select 10% of the population and calculate the percentage of all true dropouts reached in that 10% sample. We note that this is very close to 10% for all years. Because the decision rule labels less than 1% of students as dropouts as discussed above, recall at 10% is calculated for the decision rule baseline by selecting all individuals labeled as dropouts by the decision rule plus a random sample of as many individuals from the remaining population as it takes to reach a 10% sample. We then calculate the percentage of all true dropouts reached in that 10% sample.*

### Figure 4: Number of Dropouts Reached in Top 10% of Risk Scores

![alt text](https://github.com/dssg/El_Salvador_mined_education/blob/master/images/num_reached.png "Number reached")

*Figure 4 above shows the average number of true dropouts each model group would identify across the 6 prediction dates used for temporal cross-validation if we predict that the top 10% of risk scores will drop out of school. We see that each of our model groups performs considerably better than both of the baselines, identifying nearly twice as many true dropouts in the top 10% of risk scores.*


We analyze the individual features to identify the main differences between those individuals predicted to drop out by our models and those individuals not predicted to drop out. On average, individuals predicted to drop out are:

|         |                                                                             |
|--------:|:----------------------------------------------------------------------------|
|      2x |less likely to report an illness in the past three years (4.3% versus 9.1%)  | 
|177 days |above average age in their grade last year (versus 98 days younger)          |
|    1.8x |more likely to missing commute method in the last year (94% vs 51%)          |
|     10x |more grades repeated on average in the last year (.15 vs .015)               |
|      3x |more likely to have missing gender data for the previous year (48% vs 19%)   |
|      3x |more likely to have missing data for father entry (48% imputed vs 19%)       |
|     10% |fewer years spent in rural areas in past 3 years (.85 vs .95)                |



## Next Steps:

The findings show significant improvements in predicting students who are at risk of dropping out of school over the current baselines. This analysis will enable the Ministry of Education to target interventions to high risk students, as well as aggregate risk at the school level to inform funding allocation at the school level. 

To further improve the predictions, we would recommend two main next steps: first, incorporate the violence, school, and department/municipality features into model training. Second, train additional models on the expanded group of features including Naive Bayes, K-Nearest Neighbors, and Extra Trees classifiers. 


## Issues
Please use the [issue tracker](https://github.com/dssg/El_Salvador_mined_education/issues/new) to report issues or suggestions.

## Contributors

- Center for Data Sciencce and Public Policy (October 2018 - Present): Alena Stern
- Data Science for Social Good Fellows (Summer 2018): Yago del Valle-Inclán Redondo, Dhany Tjiptarto, Ana Valdivia, Mirian Lima (Project Manager)
- Technical Mentor: Adolfo De Unanue (Technical Mentor)

### Footnotes
<sup>1</sup> This is calculated by comparing the number of years since a student's last enrollment and the number of grades advanced since the last enrollment. For example, if a student were in grade 1 in 2012 and in grade 1 in 2013, we consider them to have repeated a grade as of 2013.

<sup>2</sup>The final three models selected are: 1) Random Forest Classifier: max tree depth = 50, number of trees = 100, max number of features = square root of total, minimum samples in a split = 2, class weight = null; 2) Decision Tree Classifier: max tree depth = 5, max number features = square root total, minimum samples in a split =2; 3) Scaled Logistic Regression: C: 0.001, penalty = “l2”.
