from pydantic import BaseModel, Field


# === User Models ===
class UserBase(BaseModel):
    user_id: str = Field(..., max_length=20)
    name: str
    surname: str
    age: int
    email: str
    phone: str

class User(UserBase):
    pass

class Card(BaseModel):
    card_number: str
    user_id: str


class Order(BaseModel):
    order_id: int
    quantity: int
    price_per_unit: int
    total_price: int
    card_number: str
    user_id: str
    product_id: int

class Product(BaseModel):
    product_id: int
    product_name: str    


