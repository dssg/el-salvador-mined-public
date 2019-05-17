set role el_salvador_mined_education_write;

delete from results.splits where experiment = 'testing_pipeline'; 
delete from results.learners where experiment = 'testing_pipeline';
delete from results.matrices where experiment = 'testing_pipeline';
delete from results.metrics where experiment = 'testing_pipeline';
delete from results.models where experiment = 'testing_pipeline';
delete from results.feature_importances where experiment = 'testing_pipeline';
delete from results.predictions where experiment = 'testing_pipeline';
delete from results.experiments where experiment = 'testing_pipeline';
