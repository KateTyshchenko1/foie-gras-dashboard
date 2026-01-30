"""Analyze gaps against target counts"""
import pandas as pd

# Load the restaurants data
df = pd.read_csv('restaurants_opentable.csv')

# Target counts from user
targets = {
    # Florida - 107
    'Miami': ('FL', 43),
    'Orlando': ('FL', 32),
    'Tampa': ('FL', 32),
    # Pennsylvania - 70
    'Philadelphia': ('PA', 40),
    'Pittsburgh': ('PA', 30),
    # Ohio - 60
    'Columbus': ('OH', 20),
    'Cleveland': ('OH', 20),
    'Cincinnati': ('OH', 20),
    # North Carolina - 60
    'Charlotte': ('NC', 25),
    'Raleigh': ('NC', 25),
    'Asheville': ('NC', 10),
    # New Jersey - 35
    'Jersey City': ('NJ', 15),
    'Newark': ('NJ', 10),
    'Princeton': ('NJ', 10),
    # Washington - 35
    'Seattle': ('WA', 25),
    'Tacoma': ('WA', 5),
    'Spokane': ('WA', 5),
    # Massachusetts - 30
    'Boston': ('MA', 20),
    'Cambridge': ('MA', 5),
    'Worcester': ('MA', 5),
    # Maryland - 30
    'Baltimore': ('MD', 15),
    'Bethesda': ('MD', 10),
    'Annapolis': ('MD', 5),
    # Oregon - 28
    'Portland': ('OR', 20),
    'Eugene': ('OR', 4),
    'Bend': ('OR', 4),
    # Colorado - 25
    'Denver': ('CO', 18),
    'Boulder': ('CO', 4),
    'Colorado Springs': ('CO', 3),
    # DC - 20
    'Washington': ('DC', 20),
}

print('=== CURRENT VS TARGET ===')
print("City                 State  Have   Need   Gap")
print('-' * 50)

gaps = []
total_have = 0
total_need = 0

for city, (state, target) in targets.items():
    # Count restaurants for this city (case insensitive)
    have = len(df[df['city'].str.lower() == city.lower()])
    gap = target - have
    total_have += have
    total_need += target
    
    status = 'OK' if gap <= 0 else 'NEED'
    print(f"{status:4} {city:18} {state:6} {have:6} {target:6} {gap:6}")
    
    if gap > 0:
        gaps.append((city, state, gap))

print('-' * 50)
print(f"TOTAL: Have {total_have} / Need {total_need}, Gap: {total_need - total_have}")
print()
print('=== CITIES NEEDING MORE ===')
for city, state, gap in gaps:
    print(f"  {city}, {state}: need {gap} more")
