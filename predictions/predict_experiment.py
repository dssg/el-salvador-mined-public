import sklearn
import pandas as pd
import numpy as np
import boto3
import io
import csv
from triage.component.catwalk.storage import ProjectStorage, ModelStorageEngine, MatrixStorageEngine
from triage.component.catwalk.db import connect


def to_test():
    engine = connect()
    query = "SELECT model_id, model_group_id, model_hash from model_metadata.models where model_id in (303, 292, 160)"
    model_info = engine.execute(query)
    to_test = model_info.fetchall()

    return to_test

def predict():
	
	to_test = to_test()

	matrix_storage_engine = ProjectStorage('s3://<url-to-S3-bucket>').matrix_storage_engine()
	model_storage_engine= ProjectStorage('s3://<url-to-S3-bucket>').model_storage_engine()

	matrix_uuid = '895a1b1dd28e1c9d974b8f684aec7b69'
	

	matrix_store = matrix_storage_engine.get_store(matrix_uuid)

	matrix = matrix_store.design_matrix   
	mat_index = matrix.reset_index()[['entity_id', 'as_of_date']] 

	matrix_list = []

	for model_id, model_group_id, model_hash in to_test:
	    mat_pred = mat_index
	    mat_pred['model_id'] = model_id
	    mat_pred['model_group_id'] = model_group_id
	    model = model_storage_engine.load(model_hash)
	    mat_pred['prediction'] = model.pred_proba(matrix)
	    matrix_list.append(mat_pred)

	predictions = pandas.concat(matrix_list)
	predictions.pg_copy_to('preds_final_models', engine)
	index = "CREATE INDEX entity_"

def predict_chunk(chunk_size):
    pred_models = to_test()
    pred_models.reverse()
    print(pred_models)
    model_storage_engine= ProjectStorage('s3://<url-to-S3-bucket>').model_storage_engine()
    matrix_uuid = '895a1b1dd28e1c9d974b8f684aec7b69'
    
    for model_id, model_group_id, model_hash in pred_models:
        print(model_id)
        model = model_storage_engine.load(model_hash)
        with open('{}_prediction.csv'.format(model_id),'w') as f:
            writer = csv.writer(f, delimiter= ',')
            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket='<bucket>', Key='<key>'+matrix_uuid+'.csv.gz')
            reader = pd.read_csv(io.BytesIO(obj['Body'].read()), compression = 'gzip', chunksize = chunk_size)

            for chunk in reader:
                chunk_feat = chunk.drop(columns=['entity_id', 'as_of_date', 'dropout_dummy'])
                for index, row in chunk_feat.iterrows():
                    row = np.asarray(row).reshape(1, 296)
                    prediction = model.predict_proba(row)
                    writer.writerow([model_id, model_group_id, chunk['as_of_date'][index], chunk['entity_id'][index], prediction[0][0], prediction[0][1]])
        f.close()


	

if __name__ == '__main__':
    predict_chunk(20000)

