import shutil

def save():
    shutil.copy('models/temp_model.pkl', 'models/model.pkl')
    print("✅ Model saved permanently.")
