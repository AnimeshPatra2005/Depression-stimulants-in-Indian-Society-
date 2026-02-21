from zenml import pipeline
from src.steps.ingest import skinny_extraction

@pipeline
def societal_mirror_etl():
    # Step 1: Reduce file size
    skinny_file = skinny_extraction(input_path=r"C:\Users\Lenovo\Downloads\r_TwoXIndia_posts.jsonl")
    
    # Next: Step 3 (Menta) & Step 4 (Fuzzy Logic)

if __name__ == "__main__":
    societal_mirror_etl()