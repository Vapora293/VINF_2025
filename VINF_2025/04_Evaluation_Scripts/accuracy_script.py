
import math

def calculate_ndcg(rankings, name):
    print(f"\n--- Calculating Accuracy for: {name} ---")
    dcg = 0.0
    idcg = 0.0
    
    # 1. Calculate DCG (Discounted Cumulative Gain)
    # relevance: 1.0 (Exact), 0.5 (Partial), 0.25 (View), 0.0 (Irrelevant)
    for i, item in enumerate(rankings):
        rank = i + 1
        rel = item['relevance']
        
        # Logarithmic discount (log2(rank + 1))
        discount = math.log2(rank + 1)
        gain = rel / discount
        dcg += gain
        
        print(f"Rank {rank}: {item['title']} (Rel: {rel}) -> Gain: {gain:.4f}")

    # 2. Calculate IDCG (Ideal DCG) - Best possible ordering
    # Sort items by relevance desc
    ideal_rankings = sorted(rankings, key=lambda x: x['relevance'], reverse=True)
    for i, item in enumerate(ideal_rankings):
        rank = i + 1
        rel = item['relevance']
        discount = math.log2(rank + 1)
        idcg += rel / discount
        
    accuracy = (dcg / idcg) * 100 if idcg > 0 else 0
    print(f"{'-'*30}")
    print(f"Total DCG: {dcg:.4f}")
    print(f"Ideal DCG: {idcg:.4f}")
    print(f"Final Accuracy: {accuracy:.2f}%")
    return accuracy

# DATASETS (Based on V12 Report Logic)

# 1. FLAMENCO (Geo constraint)
flamenco_data = [
    {'title': 'Teatro Flamenco Barcelona', 'relevance': 1.0},
    {'title': 'Barcelona: Flamenco Show at City Hall', 'relevance': 1.0},
    {'title': 'Tablao de Carmen', 'relevance': 1.0},
    {'title': 'Los Tarantos', 'relevance': 1.0},
    {'title': 'Palau Dalmases', 'relevance': 1.0},
    {'title': 'Barcelona: Flamenco Show with Dinner', 'relevance': 1.0},
    {'title': 'Tablao Cordobes', 'relevance': 1.0},
    {'title': 'Barcelona: VIP Flamenco Experience', 'relevance': 1.0},
    {'title': 'Flamenco Show at Tablao de Carmen (Dinner)', 'relevance': 1.0},
    {'title': 'Madrid: Local Flamenco Show', 'relevance': 0.0} # Geo mismatch
]

# 2. ARC DE TRIOMPHE (Description = 1.0)
arc_data = [
    {'title': 'Paris: Arc de Triomphe Rooftop', 'relevance': 1.0},
    {'title': 'Paris: Big Bus Hop-on Hop-off', 'relevance': 1.0}, # Validated as 1.0
    {'title': 'Arc de Triomphe & Seine Cruise', 'relevance': 1.0}, 
    {'title': 'City Tour via Amphibious Bus', 'relevance': 1.0}, 
    {'title': 'Paris: Museum Pass', 'relevance': 0.5}, # Includes it
    {'title': 'Eiffel Tower & Arc de Triomphe', 'relevance': 1.0}, 
    {'title': 'Vintage 2CV Tour', 'relevance': 1.0}, 
    {'title': 'Illumination Night Tour', 'relevance': 1.0}, 
    {'title': 'Paris: Professional Photo Shoot', 'relevance': 0.0},
    {'title': 'Champs-Elysees Walking Tour', 'relevance': 0.5} 
]

# 3. SAGRADA & PARK (Combo = 1.0, Split = 0.5)
sagrada_data = [
    {'title': 'Barcelona: Sagrada Familia and Park Güell Tour', 'relevance': 1.0},
    {'title': 'Sagrada & Park Guell Full-Day Tour', 'relevance': 1.0},
    {'title': 'Best of Barcelona: Sagrada & Park', 'relevance': 1.0},
    {'title': 'Sagrada, Park & Old Town', 'relevance': 1.0},
    {'title': 'Barcelona: Gaudi Highlights', 'relevance': 1.0}, # Covers both
    {'title': 'Park Guell: Skip-the-Line', 'relevance': 0.5}, # Missing Sagrada
    {'title': 'Sagrada Familia: Private Tour', 'relevance': 0.5}, # Missing Park
    {'title': 'E-Bike Tour (Gaudi Highlights)', 'relevance': 1.0}, # Covers both
    {'title': 'Hop-on Hop-off Bus', 'relevance': 0.0}, # Too broad
    {'title': 'Modernism Walking Tour', 'relevance': 0.0}
]

calculate_ndcg(flamenco_data, "Flamenco Show")
calculate_ndcg(arc_data, "Arc de Triomphe")
calculate_ndcg(sagrada_data, "Sagrada & Park Guell")
