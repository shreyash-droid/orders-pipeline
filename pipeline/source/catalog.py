from decimal import Decimal

CITIES = ["Chennai", "Bengaluru", "Hyderabad", "Noida", "Mumbai", "Pune", "Delhi", "Kolkata"]
SEGMENTS = ["consumer", "student", "business"]

FIRST_NAMES = ["Aarav", "Diya", "Kabir", "Meera", "Rohan", "Ananya", "Vikram", "Isha", "Arjun",
               "Sneha", "Karthik", "Priya", "Rahul", "Nisha", "Aditya", "Tara", "Farhan", "Lakshmi"]
LAST_NAMES = ["Sharma", "Iyer", "Reddy", "Khan", "Patel", "Nair", "Gupta", "Das", "Menon",
              "Singh", "Joshi", "Pillai", "Bose", "Verma", "Fernandes", "Rao"]

PRODUCTS = {
    "Electronics": [("Wireless Mouse", "799.00"), ("Mechanical Keyboard", "3499.00"),
                    ("USB-C Hub", "1899.00"), ("Noise Cancelling Headphones", "8999.00"),
                    ("Webcam 1080p", "2499.00"), ("Power Bank 20000mAh", "1599.00")],
    "Books": [("Designing Data-Intensive Applications", "1450.00"), ("The Data Warehouse Toolkit", "2100.00"),
              ("Learning Spark", "1750.00"), ("SQL Cookbook", "1200.00"), ("Clean Code", "999.00")],
    "Home": [("Desk Lamp", "1299.00"), ("Laptop Stand", "1099.00"), ("Coffee Mug", "349.00"),
             ("Office Chair Cushion", "899.00"), ("Whiteboard", "1499.00")],
    "Stationery": [("Notebook A5", "199.00"), ("Gel Pen Pack", "149.00"), ("Sticky Notes", "99.00"),
                   ("Highlighter Set", "249.00")],
}


def product_rows():
    return [(name, category, Decimal(price))
            for category, items in PRODUCTS.items() for name, price in items]


def random_customer(rng):
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    email = f"{first}.{last}{rng.randrange(1000)}@example.com".lower()
    return f"{first} {last}", email, rng.choice(CITIES), rng.choice(SEGMENTS)