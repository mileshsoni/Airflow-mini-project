import os
import pandas as pd
from sklearn.datasets import load_iris

def load_and_save():
    output_dir = 'data'
    os.makedirs(output_dir, exist_ok=True)  # ✅ create 'data/' if it doesn't exist

    iris = load_iris(as_frame=True)
    df = iris.frame
    output_path = os.path.join(output_dir, 'iris.csv')
    df.to_csv(output_path, index=False)

    print(f"✅ Data saved to {output_path}")
    
    
    
    
    
    
    