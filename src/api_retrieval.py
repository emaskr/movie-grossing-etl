import requests
import pandas as pd
import json
import random

# Load configuration
with open('config/config.json') as config_file:
    config = json.load(config_file)

API_KEY = config['api_key']  # Load the API key from config.json

def get_random_movies(page_number):
    url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&language=it-IT&sort_by=popularity.desc&include_adult=false&include_video=false&page={page_number}"
    
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()

def extract_movie_data(num_movies):
    all_movies = []
    pages = num_movies // 20 + 1  # Calculate how many pages we need to request
    
    for page in range(1, pages + 1):
        data = get_random_movies(page)
        all_movies.extend(data['results'])
    
    # Randomly sample the number of requested movies
    sampled_movies = random.sample(all_movies, num_movies)
    
    # Create a DataFrame and return it
    df = pd.DataFrame(sampled_movies)
    return df

if __name__ == "__main__":
    # Specify the number of movies to extract
    num_movies = 100  
    movie_data = extract_movie_data(num_movies)
    print(movie_data.head())
