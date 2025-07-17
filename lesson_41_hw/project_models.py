from pydantic import BaseModel, Field
import datetime

class Product(BaseModel):
    product_id: str
    product_name: str

class Order(BaseModel):
    order_id: str
    order_date: datetime.datetime

class OrderItem(BaseModel):
    item_id: str
    order_id: str
    product_id: str
    quantity: int
    price: float
    total_price: float