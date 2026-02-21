import os
import re
import json
from datetime import datetime
from zenml import step, pipeline

# ---------------------------------------------------------
# STEP DEFINITION
# ---------------------------------------------------------

@step
def skinny_extraction(input_path: str) -> str:
    """
    EXTRACT: Trims JSON and names output based on the input file name.
    """
    # 1. Get the filename from the path (e.g., 'r_TeenIndia_posts.jsonl')
    base_name = os.path.basename(input_path)
    
    # 2. Remove the extension and add '_result'
    file_name_only = os.path.splitext(base_name)[0]
    output_filename = f"{file_name_only}_result.jsonl"
    
    # 3. Set the full output path and ensure directory exists
    os.makedirs("data/interim", exist_ok=True)
    output_path = os.path.join("data/interim", output_filename)

    count = 0
    with open(input_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'w', encoding='utf-8') as outfile:
        
        for line in infile:
            try:
                raw = json.loads(line)
                
                # Combine title and body
                text_content = f"{raw.get('title', '')} {raw.get('selftext', '')}".strip()
                
                # Safety check: Only process if there is actual text
                if text_content:
                    ts = raw.get("created_utc")
                    if ts:
                        dt = datetime.fromtimestamp(ts)
                        skinny = {
                            "text": text_content,
                            "date": dt.strftime('%d-%m-%Y'),
                            "year": dt.year,
                        }
                        outfile.write(json.dumps(skinny) + "\n")
                        count += 1
            except Exception:
                continue

    print(f"Extraction complete! Processed {count} posts.")
    print(f"Saved to: {output_path}")
    return output_path