import json
import os
import re

# The regex for filtering
DEPRESSION_REGEX = re.compile(
    r'\b('
    r'depress(?:ion|ed|ing)?|suicid(e|al)|hopeless|therapy|'
    r'mental\s+health|anxiety|breakdown|lonely|isolat(?:ed|ion)|'
    r'sad|miserable|antidepressant|self\s*[- ]?harm|overwhelm(?:ed|ing)|'
    r'burnout|crime|issues|stress|psychiatrist|monotonous|bleak|'
    r'cr(?:y|ies|ied|ying)|wept|sobbing' 
    r')\b',
    re.IGNORECASE
)

def filter_jsonl(input_path):
    # 1. Generate the output filename
    base_name = os.path.basename(input_path)
    file_name_only = os.path.splitext(base_name)[0]
    output_path = f"{file_name_only}_regex_cleaned.jsonl"

    print(f"Starting to process: {base_name}...")
    
    total, kept = 0, 0

    try:
        with open(input_path, 'r', encoding='utf-8') as infile, \
             open(output_path, 'w', encoding='utf-8') as outfile:
            
            for line in infile:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    data = json.loads(line)
                    text = data.get('text', '').strip()
                    total += 1
                    
                    if text and DEPRESSION_REGEX.search(text):
                        outfile.write(json.dumps(data) + "\n")
                        kept += 1
                except json.JSONDecodeError:
                    continue

        print("-" * 30)
        print(f"Success!")
        print(f"Total posts scanned: {total}")
        print(f"Matching posts kept: {kept}")
        print(f"Output saved to: {os.path.abspath(output_path)}")
        print("-" * 30)

    except FileNotFoundError:
        print(f"Error: Could not find the file at {input_path}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    target_file = r"C:\Users\BIT\Desktop\exam\r_OffMyChestIndia_posts_result.jsonl"
    
    filter_jsonl(target_file)