# Student Name: Mohammed Al-Zubiri
# Article Similarity Assignment

import csv
import pickle
import numpy as np
import sys
import os

# Import functions from cosine_similarity module
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'cosine_similarity'))
from cosine_similarity import clean_text, generate_vector, cosine_similarity


def read_articles(filepath):
    articles = []
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            articles.append({
                'id': int(row['id']),
                'title': row['title'].strip(),
                'content': row['content'].strip()
            })
    return articles


def build_global_vocabulary(cleaned_texts):
    """Build a sorted global vocabulary from all cleaned article texts."""
    all_words = []
    for text in cleaned_texts:
        all_words += text.split()
    vocabulary = sorted(list(set(all_words)))
    return vocabulary


def calculate_similarity_matrix(vectors):
    n = len(vectors)
    matrix = np.zeros((n, n))
    for i in range(n):
        for j in range(n):
            matrix[i][j] = cosine_similarity(np.array(vectors[i]), np.array(vectors[j]))
    return matrix


def save_similarity_matrix(matrix, filepath):
    with open(filepath, 'wb') as f:
        pickle.dump(matrix, f)


def find_most_similar(article_id, articles, similarity_matrix, top_n=3):
    idx = None
    for i, article in enumerate(articles):
        if article['id'] == article_id:
            idx = i
            break

    if idx is None:
        return []

    similarities = []
    for i in range(len(articles)):
        if i != idx:
            similarities.append((i, similarity_matrix[idx][i]))

    similarities.sort(key=lambda x: x[1], reverse=True)

    result = []
    for i in range(min(top_n, len(similarities))):
        result.append(articles[similarities[i][0]]['title'])

    return result


if __name__ == '__main__':
    # Step 1: Read articles
    csv_path = os.path.join(os.path.dirname(__file__), 'articles.csv')
    articles = read_articles(csv_path)

    # Step 2: Clean article content
    cleaned_texts = []
    for article in articles:
        cleaned_texts.append(clean_text(article['content']))

    # Step 3: Build global bag-of-words vocabulary
    vocabulary = build_global_vocabulary(cleaned_texts)

    # Step 4: Build vector representation for each article
    vectors = []
    for text in cleaned_texts:
        vector = generate_vector(vocabulary, text)
        vectors.append(vector)

    # Step 5: Calculate cosine similarity matrix
    similarity_matrix = calculate_similarity_matrix(vectors)

    # Step 6: Save similarity matrix to pickle file
    pkl_path = os.path.join(os.path.dirname(__file__), 'similarities.pkl')
    save_similarity_matrix(similarity_matrix, pkl_path)

    # Step 7: Print similarity matrix and most similar articles
    print("Similarity Matrix:")
    print(similarity_matrix)
    print()

    for article in articles:
        similar = find_most_similar(article['id'], articles, similarity_matrix)
        print(f"Most similar to '{article['title']}':")
        for i, title in enumerate(similar, 1):
            print(f"  {i}. {title}")
        print()
