set role el_salvador_mined_education_write;

delete from results.experiments where experiment = ':experiment';
delete from results.splits where experiment = ':experiment';
delete from results.learners where experiment = ':experiment';
delete from results.models where experiment = ':experiment';
delete from results.feature_importances where experiment = ':experiment';
delete from results.matrices where experiment = ':experiment';
delete from results.predictions where experiment = ':experiment';
delete from results.metrics where experiment = ':experiment';
