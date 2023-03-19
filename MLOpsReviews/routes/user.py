from fastapi import APIRouter

user = APIRouter()

@user.get('/users')
def hello_world():
    return 'Hello World'
