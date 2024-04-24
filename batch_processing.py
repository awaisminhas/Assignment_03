import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

nltk.download('punkt', download_dir='/usr/share/nltk_data')
nltk.download('stopwords', download_dir='/usr/share/nltk_data')
nltk.download('wordnet', download_dir='/usr/share/nltk_data')

def preprocess_text(text):
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

def batch_preprocess_text(texts):
    preprocessed_texts = []
    for text in texts:
        preprocessed_text = preprocess_text(text)
        preprocessed_texts.append(preprocessed_text)
    return preprocessed_texts

def read_texts_from_file(file_path):
    with open(file_path, 'r') as file:
        texts = file.readlines()
    return texts

def write_preprocessed_texts_to_file(preprocessed_texts, output_file_path):
    with open(output_file_path, 'w') as file:
        for text in preprocessed_texts:
            file.write(text + '\n')

# Example text file path
input_file_path = '/content/input.txt'
output_file_path = 'preprocessed_text.txt'

# Read texts from file
texts = read_texts_from_file(input_file_path)

# Preprocess the texts
preprocessed_texts = batch_preprocess_text(texts)

# Write the preprocessed texts to a new file
write_preprocessed_texts_to_file(preprocessed_texts, output_file_path)

# Print the preprocessed texts
for index, text in enumerate(preprocessed_texts):
    print(f"Text {index+1}: {text}")
