import pandas as pd

df_rest = pd.read_csv('restaurants_opentable.csv')
df_menu = pd.read_csv('menu_items_opentable.csv')

foie = df_menu[df_menu['title'].str.lower().str.contains('foie', na=False)]

print("=== FINAL SUMMARY ===")
print(f"Total restaurants: {len(df_rest):,}")
print(f"Total menu items: {len(df_menu):,}")
print(f"Foie gras items: {len(foie)}")
print()

print("Sample foie gras dishes:")
for idx, row in foie.head(10).iterrows():
    price = f"${row['price']}" if pd.notna(row['price']) and row['price'] else 'N/A'
    print(f"  - {row['title']} ({price})")
