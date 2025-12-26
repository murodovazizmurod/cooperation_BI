import json

with open('parsed_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"Categories parsed: {len(data)}")
total_offers = sum(len(cat['offers']) for cat in data.values())
print(f"Total offers parsed: {total_offers}")
print("\nBreakdown by category:")
for cat_id, cat in data.items():
    name = cat['category_name'][:60] + "..." if len(cat['category_name']) > 60 else cat['category_name']
    print(f"  Category {int(cat_id):2d}: {len(cat['offers']):3d} offers parsed (total available: {cat['total']:,}) - {name}")

