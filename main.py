import os
from multiprocessing import Pool
from typing import Union, List, Dict
import re
import requests
from datetime import datetime
from pathlib import Path

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

def get_2ch_boards() -> List[Dict]:
    """Получение списка досок"""
    url = "https://2ch.hk/index.json"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        #return [{'id':'b'}, {'id':'ya'}]
        return response.json().get('boards', [])
    except Exception as e:
        print(f"Error getting boards: {e}")
        return []

def get_threads(board: str) -> List[List[Union[str, int]]]:
    print(f"Getting threads from {board}...")
    try:
        response = requests.get(f"https://2ch.hk/{board}/catalog.json").json()
    except Exception as e:
        print(f"Error fetching catalog for {board}: {e}")
        return []

    threads = []
    for thread in response.get('threads', []):
        threads.append([board, thread.get('num')])
    return threads


def remove_tag_a(text: str) -> str:
    return re.sub(r'<a.*?</a>', '', text)


def fetch_posts(args: List[Union[str, int]]) -> List[str]:
    board, thread_id = args
    print(f"Fetching thread {thread_id} from {board}...")
    try:
        response = requests.get(f"https://2ch.hk/{board}/res/{thread_id}.json").json()
    except Exception as e:
        print(f"Error fetching thread {thread_id} on {board}: {e}")
        return []

    posts = []
    for post in response.get('threads', [])[0].get('posts', []):
        comment = post.get('comment', '')
        # Split after links, take last segment
        parts = comment.split('</a><br>')
        text = parts[-1] if parts else comment

        if text:
            # Clean tags and entities
            text = (text.replace('<span class="spoiler">', '')
                        .replace('</span>', '')
                        .replace('<span class="unkfunc">', '')
                        .replace('<span class="s">', '')
                        .replace('<br>', ' ')
                        .replace('&lt;', '<')
                        .replace('&gt;', '>')
                        .replace('<strong>', '').replace('</strong>', '')
                        .replace('<em>', '').replace('</em>', '')
                        .replace('<p>', '').replace('</p>', '')
                        .replace('<b>', '').replace('</b>', '')
                        .replace('&#47;', '/')
                        .replace('&quot;', '"')
                        .replace('\xa0', ' '))
            text = remove_tag_a(text)
            # Remove any stray angle brackets
            text = text.replace('<', '').replace('>', '')
            posts.append(text)
    return posts


def save_to_file(directory: Path, board: str, data: List[str]) -> None:
    filename = directory / f"{board}.txt"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("\n".join(data))
    print(f"Saved {len(data)} posts to {filename}")


if __name__ == '__main__':
    # Specify boards to parse
    boards = [board['id'] for board in get_2ch_boards()]

    # Prepare timestamped output directory
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_root = Path('storage')
    output_dir = output_root / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}")

    # Gather all threads
    all_threads = []
    for board in boards:
        all_threads.extend(get_threads(board))

    print(f"Total threads to fetch: {len(all_threads)}")

    # Fetch posts in parallel
    with Pool() as pool:
        results = pool.map(fetch_posts, all_threads)

    # Group results by board
    grouped: dict[str, List[str]] = {}
    for (board, _), posts in zip(all_threads, results):
        grouped.setdefault(board, []).extend(posts)

    # Save each board's posts to its own file
    for board, posts in grouped.items():
        save_to_file(output_dir, board, posts)
