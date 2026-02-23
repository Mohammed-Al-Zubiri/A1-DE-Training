import numpy as np
import re

def clean_text(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    return text

def generate_bag_of_words(text):
    words = text.split()
    bag_of_words = []
    for word in words:
        if word not in bag_of_words:
            bag_of_words.append(word)
    return bag_of_words

def generate_vector(bag_of_words, text):
    vector = []
    for word in bag_of_words:
        word_count = text.count(word)
        vector.append(word_count)
    return vector

def cosine_similarity(vec1, vec2):
    dot_product = np.dot(vec1, vec2)
    magnitude_vec1 = np.linalg.norm(vec1)
    magnitude_vec2 = np.linalg.norm(vec2)
    if magnitude_vec1 == 0 or magnitude_vec2 == 0:
        return 0.0
    return dot_product / (magnitude_vec1 * magnitude_vec2)
