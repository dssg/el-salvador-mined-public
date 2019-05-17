from triage.component.catwalk.db import connect


for model_id in [292, 303, 160]:
    engine = connect()

    print(model_id)
    drop_table = "DROP TABLE if exists predictions.predictions_{};".format(model_id)
    create_table = "CREATE TABLE predictions.predictions_{} (model_id smallint, model_group_id smallint, as_of_date date, entity_id int NOT NULL, prediction_0 real, prediction_1 real);".format(model_id)

    engine.execute(drop_table)
    engine.execute(create_table)
