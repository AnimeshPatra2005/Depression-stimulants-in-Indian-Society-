import json
import os
import torch
import time
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from torch.utils.data import DataLoader, Dataset

# Configuration
MODEL_ID = "rafalposwiata/deproberta-large-depression"
MAX_CHARS = 2000
FRONT_CHARS = 1000
BACK_CHARS = 1000

def _text_for_model(text: str) -> str:
   
    if len(text) <= MAX_CHARS:
        return text
    return text[:FRONT_CHARS].strip() + " " + text[-BACK_CHARS:].strip()

class JSONLDataset(Dataset):
    def __init__(self, input_path):
        with open(input_path, 'r', encoding='utf-8') as f:
            self.lines = f.readlines()
            
    def __len__(self):
        return len(self.lines)
        
    def __getitem__(self, idx):
        try:
            data = json.loads(self.lines[idx])
            text = data.get('text', '').strip()
            return {"text": _text_for_model(text), "original_data": self.lines[idx]}
        except Exception:
            return {"text": "", "original_data": "{}"}

def slm_filter_depression_posts(input_path: str, batch_size: int = 64):
    base_file = os.path.basename(input_path).replace('.jsonl', '_filtered.jsonl')
    output_path = f"/kaggle/working/{base_file}"

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"🚀 Using device: {device.upper()}")
    
    # Load Model and Tokenizer
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_ID, 
        torch_dtype=torch.float16 if device == "cuda" else torch.float32
    ).to(device).eval()

    # Filtering logic: Keep everything NOT labeled as 'not-depression'
    id2label = model.config.id2label
    keep_ids = [i for i, l in id2label.items() if "not" not in l.lower()]
    print(f"Target Labels: {[id2label[i] for i in keep_ids]}")

    # Prepare Data
    dataset = JSONLDataset(input_path)
    dataloader = DataLoader(dataset, batch_size=batch_size, num_workers=2, pin_memory=True)

    total, kept = 0, 0
    start_time = time.time()

    with open(output_path, 'w', encoding='utf-8') as outfile:
        with torch.inference_mode():
            for batch in tqdm(dataloader, desc="Processing Posts"):
                # Skip empty batches
                if not batch["text"]: continue
                
                # Tokenization
                enc = tokenizer(
                    batch["text"], 
                    padding=True, 
                    truncation=True, 
                    max_length=512, 
                    return_tensors="pt"
                ).to(device)
                
                # Inference
                outputs = model(**enc)
                preds = outputs.logits.argmax(dim=-1).cpu().tolist()

                # Process results
                for i, pred in enumerate(preds):
                    total += 1
                    if pred in keep_ids:
                        try:
                            meta = json.loads(batch["original_data"][i])
                            meta["depression_label"] = id2label[pred]
                            outfile.write(json.dumps(meta) + "\n")
                            kept += 1
                        except:
                            continue

    end_time = time.time()
    duration = end_time - start_time
    
    print("\n" + "="*40)
    print(f"COMPLETED IN: {duration:.2f} seconds")
    print(f"Total Scanned: {total}")
    print(f"Depressive Posts Kept: {kept}")
    print(f"Saved to: {output_path}")
    print("="*40)

if __name__ == "__main__":
    # Update this path to your actual Kaggle input path
    my_input = r"/kaggle/input/datasets/parthanimesh/roffmychest/r_OffMyChestIndia_posts_result_regex_cleaned.jsonl"
    
    # Check if file exists before running
    if os.path.exists(my_input):
        slm_filter_depression_posts(my_input, batch_size=64)
    else:
        print(f"Error: File not found at {my_input}")