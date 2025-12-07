import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

import pyodbc
from dotenv import load_dotenv

from fastapi import FastAPI
from pydantic import BaseModel

# ----------------------------
#  ENV & CONFIG
# ----------------------------

load_dotenv()

CONNECTION_STRING = os.getenv("AZURE_SQL_CONNECTION_STRING")

if not CONNECTION_STRING:
    raise ValueError("AZURE_SQL_CONNECTION_STRING is not set in environment/.env")

# ----------------------------
#  DATA MODELS (dataclasses)
# ----------------------------

@dataclass
class User:
    userId: int
    firstName: str
    lastName: str
    totalRentals: int


@dataclass
class Equipment:
    equipmentId: int
    name: str
    category: str
    totalRentals: int
    equipmentStatus: str


@dataclass
class Rental:
    rentalId: int
    dateStart: datetime
    dateFinish: Optional[datetime]
    expectedReturn: datetime
    actualReturn: Optional[datetime]
    status: str
    userId: int
    equipmentId: int


# ----------------------------
#  API MODELS (Pydantic)
# ----------------------------

class UserOut(BaseModel):
    userId: int
    firstName: str
    lastName: str
    totalRentals: int


class EquipmentOut(BaseModel):
    equipmentId: int
    name: str
    category: str
    totalRentals: int
    equipmentStatus: str


class RentalOut(BaseModel):
    rentalId: int
    dateStart: datetime
    dateFinish: Optional[datetime]
    expectedReturn: datetime
    actualReturn: Optional[datetime]
    status: str
    userId: int
    equipmentId: int


# ----------------------------
#  DB CONNECTION (pyodbc)
# ----------------------------

def get_connection() -> pyodbc.Connection:
    return pyodbc.connect(CONNECTION_STRING)


# ----------------------------
#  QUERIES
# ----------------------------

def fetch_users(conn: pyodbc.Connection) -> List[User]:
    cursor = conn.cursor()
    cursor.execute("SELECT userId, firstName, lastName, totalRentals FROM dbo.[User];")
    return [
        User(
            userId=row[0],
            firstName=row[1],
            lastName=row[2],
            totalRentals=row[3],
        )
        for row in cursor.fetchall()
    ]


def fetch_equipments(conn: pyodbc.Connection) -> List[Equipment]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT equipmentId, name, category, totalRentals, equipmentStatus
        FROM dbo.Equipment;
        """
    )
    return [
        Equipment(
            equipmentId=row[0],
            name=row[1],
            category=row[2],
            totalRentals=row[3],
            equipmentStatus=row[4],
        )
        for row in cursor.fetchall()
    ]


def fetch_rentals(conn: pyodbc.Connection) -> List[Rental]:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT 
            rentalId,
            dateStart,
            dateFinish,
            expectedReturn,
            actualReturn,
            status,
            userId,
            equipmentId
        FROM dbo.Rental;
        """
    )
    return [
        Rental(
            rentalId=row[0],
            dateStart=row[1],
            dateFinish=row[2],
            expectedReturn=row[3],
            actualReturn=row[4],
            status=row[5],
            userId=row[6],
            equipmentId=row[7],
        )
        for row in cursor.fetchall()
    ]


# ----------------------------
#  FASTAPI APP & ROUTES
# ----------------------------

app = FastAPI()


@app.get("/")
def root():
    print("Root of Rental API")
    return "Rental API is running"


@app.get("/users", response_model=List[UserOut])
def get_users():
    with get_connection() as conn:
        users = fetch_users(conn)

    print("=== USERS (via /users) ===")
    for u in users:
        print(u)

    return [UserOut(**u.__dict__) for u in users]


@app.get("/equipment", response_model=List[EquipmentOut])
def get_equipment():
    with get_connection() as conn:
        equipment = fetch_equipments(conn)

    print("=== EQUIPMENTS (via /equipment) ===")
    for e in equipment:
        print(e)

    return [EquipmentOut(**e.__dict__) for e in equipment]


@app.get("/rentals", response_model=List[RentalOut])
def get_rentals():
    with get_connection() as conn:
        rentals = fetch_rentals(conn)

    print("=== RENTALS (via /rentals) ===")
    for r in rentals:
        print(r)

    return [RentalOut(**r.__dict__) for r in rentals]


# ----------------------------
#  MAIN 
# ----------------------------

def main():
    with get_connection() as conn:
        print("=== USERS ===")
        users = fetch_users(conn)
        for u in users:
            print(u)

        print("\n=== EQUIPMENTS ===")
        equipment = fetch_equipments(conn)
        for e in equipment:
            print(e)

        print("\n=== RENTALS ===")
        rentals = fetch_rentals(conn)
        for r in rentals:
            print(r)


if __name__ == "__main__":
    main()
