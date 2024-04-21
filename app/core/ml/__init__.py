from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import Normalizer
import numpy as np
import pandas as pd

def tensor_to_array(string):
    string = string[8:-2]
    array_ = string.split(',')
    return np.array(array_, dtype=int).mean()

class Model_Predictor():
    model = None
    logreg = LogisticRegression()
    predict_labels = ['gender_x', 'age']
    categories_labels = None
    def __init__(self, model_, metric_, categories_labels_, value_labels_, tensor_labels_, logreg_ = LogisticRegression()) -> None:
        self.model = model_
        self.metric = metric_
        self.categories_labels = categories_labels_
        self.tensor_labels = tensor_labels_
        self.logregs = None
        self.value_labels = value_labels_
        self.normalizers = None
        self.min_y_val = None
    def convert_X(self, X = None):
        X_new = pd.get_dummies(X, columns = self.categories_labels)
        return X_new
        
    def fit(self, X, y):
        X = X.copy()
        X = self.convert_X(X)
        self.normalizers = [0]*len(self.value_labels)
        for i, label_ in enumerate(self.value_labels):
            self.normalizers[i] = (X[label_].mean(), X[label_].std())
            mean_, std_ = self.normalizers[i]
            X[label_] = (X[label_] - mean_)/std_

        self.tensor_normalizers = [0]*len(self.tensor_labels)
        for i, label_ in enumerate(self.tensor_labels):
            X[label_] = X[label_].apply(tensor_to_array)
            self.tensor_normalizers[i] = (X[label_].mean(), X[label_].std())
            mean_, std_ = self.tensor_normalizers[i]
            X[label_] = (X[label_] - mean_)/std_
        self.min_y_val = np.min(y)
        self.model.fit(X, y - np.min(y))
        self.colums = X.columns
        return self
    def predict(self, X, y):
        X = X.copy()
        X = self.convert_X(X)
        for i, label_ in enumerate(self.value_labels):
            mean_, std_ = self.normalizers[i]
            X[label_] = (X[label_] - mean_)/std_
            
        for i, label_ in enumerate(self.tensor_labels):
            X[label_] = X[label_].apply(tensor_to_array)
            self.tensor_normalizers[i] = (X[label_].mean(), X[label_].std())
            mean_, std_ = self.tensor_normalizers[i]
            X[label_] = (X[label_] - mean_)/std_
        return self.metric(self.model.predict(X) + self.min_y_val, y)
