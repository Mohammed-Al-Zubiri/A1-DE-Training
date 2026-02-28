import numpy as np
import re

def clean_text(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\d+', '', text)
    return text

def generate_vocabulary(text1, text2):
    words1 = text1.split()
    words2 = text2.split()
    all_words = words1 + words2
    vocabulary = sorted(list(set(all_words)))
    return vocabulary

def generate_vector(vocabulary, text):
    words = text.split()
    vector = []
    for vocab_word in vocabulary:
        word_count = words.count(vocab_word) 
        vector.append(word_count)
    return vector

def cosine_similarity(vec1, vec2):
    dot_product = np.dot(vec1, vec2)
    magnitude_vec1 = np.linalg.norm(vec1)
    magnitude_vec2 = np.linalg.norm(vec2)
    if magnitude_vec1 == 0 or magnitude_vec2 == 0:
        return 0.0
    return dot_product / (magnitude_vec1 * magnitude_vec2)
