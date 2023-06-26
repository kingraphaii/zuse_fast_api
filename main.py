import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import httpx
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SQLALCHEMY_DATABASE_URL = "sqlite:///./database.db"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Data(Base):
    __tablename__ = "data"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, index=True)
    body = Column(String)


Base.metadata.create_all(bind=engine)


@app.get("/fetch-data")
async def fetch_data():
    async with httpx.AsyncClient() as client:
        response = await client.get("https://jsonplaceholder.typicode.com/posts")
        posts = response.json()

    session = SessionLocal()

    try:
        for post in posts:
            existing_entry = session.query(Data).filter_by(title=post["title"]).first()

            if existing_entry:
                # Entry already exists, update it
                existing_entry.body = post["body"]
            else:
                # Entry does not exist, create a new entry
                data_entry = Data(title=post["title"], body=post["body"])
                session.add(data_entry)

        session.commit()
        return {"message": "Data stored successfully!"}

    except IntegrityError:
        session.rollback()
        return {"message": "IntegrityError occurred while storing data"}

    finally:
        session.close()


@app.websocket("/ws/ping")
async def websocket_ping_pong(websocket: WebSocket):
    await websocket.accept()

    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Pong! You sent: {data}")
            logger.info(f"Received message: {data}")
    except WebSocketDisconnect as ex:
        logger.error(f"WebSocket disconnected: {ex}")


@app.websocket("/ws/echo")
async def websocket_echo(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(data)
            logger.info(f"Received message: {data}")
    except WebSocketDisconnect as ex:
        logger.error(f"WebSocket disconnected: {ex}")
