#!/usr/bin/env python3

import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from llm_engineering.application.crawlers.substack import SubstackCrawler


def test_substack_crawler():
    """Test the SubstackCrawler with a sample URL."""
    crawler = SubstackCrawler()
    
    # Test with the Substack article URL
    url = "https://maximelabonne.substack.com/p/uncensor-any-llm-with-abliteration-d30148b7d43e"
    
    print(f"Testing SubstackCrawler with URL: {url}")
    try:
        data = crawler.extract(url)
        print("Extraction successful!")
        print(f"Title: {data.get('Title')}")
        print(f"Subtitle: {data.get('Subtitle')}")
        print(f"Content length: {len(data.get('Content', ''))}")
        
        # Optionally save to a file
        with open("extracted_content.txt", "w", encoding="utf-8") as f:
            f.write(f"Title: {data.get('Title')}\n\n")
            f.write(f"Subtitle: {data.get('Subtitle')}\n\n")
            f.write(f"Content:\n{data.get('Content')}")
        
        print("Content saved to extracted_content.txt")
        
    except Exception as e:
        print(f"Error during extraction: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_substack_crawler()

