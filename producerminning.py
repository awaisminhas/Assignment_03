import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import time

# Ensure NLTK data is downloaded
nltk.download('punkt', download_dir='/usr/share/nltk_data')
nltk.download('stopwords', download_dir='/usr/share/nltk_data')
nltk.download('wordnet', download_dir='/usr/share/nltk_data')

def preprocess_document(text):
    # Convert text to lowercase
    text = text.lower()
    
    # Remove special characters and numbers
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    
    # Tokenization
    tokens = word_tokenize(text)
    
    # Remove stopwords
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [word for word in tokens if word not in stop_words]
    
    # Lemmatization
    lemmatizer = WordNetLemmatizer()
    lemmatized_tokens = [lemmatizer.lemmatize(word) for word in filtered_tokens]
    
    # Join the tokens back into a single string
    preprocessed_text = ' '.join(lemmatized_tokens)
    
    return preprocessed_text

def read_text_from_file(file_path):
    with open(file_path, 'r') as file:
        texts = file.readlines()
    return texts

# Example text file path
input_file_path = '/content/preprocessed_text.txt'

# Read texts from file
texts = read_text_from_file(input_file_path)

# Preprocess the texts
preprocessed_documents = [preprocess_document(text) for text in texts]

# Stream the preprocessed texts in real-time
def stream_preprocessed_data(preprocessed_documents):
    for document in preprocessed_documents:
        yield document
        time.sleep(1)  

if _name_ == "main":
    for data in stream_preprocessed_data(preprocessed_documents):
        print(data)
