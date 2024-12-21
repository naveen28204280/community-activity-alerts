from flask import Flask, render_template, request, jsonify
from datetime import datetime
import sqlite3
import json
import requests

app = Flask(__name__)

def get_all_communities():
    url = 'https://commons.wikimedia.org/w/api.php?action=sitematrix&smtype=language&format=json'
    response = requests.get(url)
    data = response.json()
    sitematrix = data['sitematrix']
    
    # Extract languages and their communities
    languages = {}
    for key, value in sitematrix.items():
        if key.isdigit() and 'name' in value:
            communities = [
                {"sitename": site['sitename'], "url": site['url']}
                for site in value.get('site', [])
            ]
            languages[value['name']] = communities

    return languages


@app.route('/')
def index():
    language = request.args.get('language')
    project_group = request.args.get('project_group')
    datestart= request.args.get('datestart')
    dateend = request.args.get('dateend')
    filter_edits = request.args.get('filter_edits') == 'true'
    filter_users = request.args.get('filter_users') == 'true'

    # Process the data as needed
    print(f"Language: {language}")
    print(f"Project Group: {project_group}")
    print(f"Date Start: {datestart}")
    print(f"Date End: {dateend}")
    print(f"Filter Edits: {filter_edits}")
    print(f"Filter Users: {filter_users}")

    languages = get_all_communities()
    return render_template('index.html', languages=languages)

@app.route('/search')
def search():
    query = request.args.get('query', '').lower()
    communities = get_all_communities()
    # Filter communities based on query
    filtered_communities = [
        value['name'] for key, value in communities.items()
        if 'name' in value and query in value['name'].lower()
    ]
    return jsonify(filtered_communities)



if __name__ == '__main__':
    app.run(debug=True)