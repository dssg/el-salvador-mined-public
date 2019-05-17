set role el_salvador_mined_education_write;

delete from results.splits where experiment = :exp; 
delete from results.learners where experiment = :exp;
delete from results.matrices where experiment = :exp;
delete from results.metrics where experiment = :exp;
delete from results.models where experiment = :exp;
delete from results.models2 where experiment = :exp;
delete from results.feature_importances2 where experiment = :exp;
delete from results.predictions where experiment = :exp;
delete from results.experiments where experiment = :exp;