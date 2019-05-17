from triage.component.catwalk.db import connect
import pandas as pd


def pred_list(model_id):
    engine = connect()
    query = '''SELECT entity_id, pred_rank from 
    (SELECT entity_id, prediction_1,
    PERCENT_RANK() OVER(ORDER BY prediction_1 DESC) as pc,
    ROW_NUMBER() OVER(ORDER BY prediction_1 DESc) as pred_rank 
    FROM predictions.predictions_{}) as pc_tbl
    WHERE pc <= 0.1
    ORDER BY pred_rank;'''.format(model_id)
    model_info = engine.execute(query)
    data = model_info.fetchall()
    df = pd.DataFrame(data)
    df.rename(index=str, columns={0:'entity_id', 1: 'pred_rank'}, inplace=True)
    df.to_csv('pred_list_{}.csv'.format(model_id), index=False)

if __name__ == "__main__":
    for model_id in [303, 292, 160]:
        pred_list(model_id)


    