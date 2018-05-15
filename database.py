import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import redis


def initialize():
    global customers
    global products
    global orders
    global myRedis
    client = MongoClient()
    r = redis.StrictRedis()
    myRedis = r
    customers = client.dbdcmps364.Customers
    products = client.dbdcmps364.Products
    orders = client.dbdcmps364.Orders

def get_customers():
    allcustomers = customers.find({})
    customerlist = []
    for i in range(0, allcustomers.count(), 1):
        customerlist.append(allcustomers[i])
    return customerlist

def get_customer(id):
    object_id = ObjectId(id)
    specCustomer = customers.find_one({'_id' : object_id})
    return specCustomer

def upsert_customer(customer):
    if customer.get('_id') == None:
        customers.insert_one(customer)
    else:
        object_id = ObjectId(customer['_id'])
        customers.update_one({'_id' : object_id}, {'$set' : {'firstName' : customer['firstName'], 'lastName' : customer['lastName'], 
        'street' : customer['street'], 'city' : customer['city'], 'state' : customer['state'], 'zip' : customer['zip']}})
    return None

def delete_customer(id):
    object_id = ObjectId(id)
    customers.delete_one({'_id' : object_id})
    return None


def get_products():
    allproducts = products.find({})
    productlist = []
    for i in range(0, allproducts.count(), 1):
        productlist.append(allproducts[i])

    return productlist

def get_product(id):
    object_id = ObjectId(id)
    specProduct = products.find_one({'_id' : object_id})

    return specProduct

def upsert_product(product):
    if product.get('_id') == None:
        products.insert_one(product)
    else:
        object_id = ObjectId(product['_id'])
        products.update_one({'_id' : object_id}, {'$set' : {'name' : product['name'], 'price' : product['price']}})
    return None

def delete_product(id):
    object_id = ObjectId(id)
    products.delete_one({'_id' : object_id})
    return None


def get_orders():
    allorders = orders.find({})
    orderlist = []
    for i in range(0, allorders.count(), 1):
        orderlist.append(allorders[i])
    return orderlist

def get_order(id):
    object_id = ObjectId(id)
    specOrder = orders.find_one({'_id': object_id})
    return specOrder

def upsert_order(order):
    customer = {}
    CustId = ObjectId(order['customerId'])
    document1 = customers.find_one({'_id' : CustId})
    customer['firstName'] = document1['firstName']
    customer['lastName'] = document1['lastName']
    order['customer'] = customer

    product = {}
    ProdId = ObjectId(order['productId'])
    documnet2 = products.find_one({'_id' : ProdId})
    product['name'] = documnet2['name']
    order['product'] = product

    if myRedis.exists(ObjectId(order['productId'])) != None:
        myRedis.delete(ObjectId(order['productId']))

    del order['day']
    del order['month']
    del order['year']
    del order['customerId']

    orders.insert_one(order)
    return None

def delete_order(id):
    object_id = ObjectId(id)
    deletingorder = orders.find_one({'_id': object_id})
    deletingorder['productId']

    if myRedis.exists(ObjectId(deletingorder['productId'])) != False:
        myRedis.delete(ObjectId(deletingorder['productId']))

    orders.delete_one({'_id': object_id})
    return None


def sales_report():
    saleslist = []
    allproducts = products.find({})

    for some_product in allproducts:
        DateofSale = "0001-01-01"
        total = 0
        prodId = ObjectId(some_product['_id'])

        if myRedis.exists(prodId) == False:
            prodName = some_product['name'] 
            price = some_product['price']
            NameCount = orders.find({'product.name': prodName})

            for names in NameCount:
                total = total + 1

            dates = orders.find({'product.name': prodName})
            for date in dates:
                if date['date'] > DateofSale:
                    DateofSale = date['date']

            document = {}
            document['name'] = prodName
            document['total_sales'] = total
            document['gross_revenue'] = total * price
            document['last_order_date'] = DateofSale

            myRedis.hmset(prodId, document)
            saleslist.append(myRedis.hgetall(prodId))
        else:
            saleslist.append(myRedis.hgetall(prodId))

    return saleslist




