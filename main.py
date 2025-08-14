import os
import pandas as pd
import time
from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Depends, status, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Optional, List, Dict
import unicodedata
import json
import requests
import math
import random
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# --- Configuration ---
# JWT
SECRET_KEY = "your-secret-key-1234567890"  # Replace with a secure key in production
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60
GOOGLE_API_KEY = "AIzaSyCdmDUU7PtIOVc3hrdszc5gaxURG_2daNQ"  # Replace with your real API key


# Geospatial
BASE_LOCATION_LATITUDE = 51.25881
BASE_LOCATION_LONGITUDE = 6.39868
RADIUS_KM = 10
MIN_RADIUS_KM = 2

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Mock user database
USERS = {
    "admin": {"username": "admin", "password": pwd_context.hash("securepassword123")}
}

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# --- Utility Functions ---
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Creates a JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def _generate_random_location_in_radius(base_lat, base_lon, radius_km, min_radius_km=2):
    """
    Generates random coordinates within a given radius (and above a min radius) from the base point.
    """
    R = 6371  # Earth radius in km
    base_lat_rad = math.radians(base_lat)
    base_lon_rad = math.radians(base_lon)

    # Random distance within the given range
    distance_km = random.uniform(min_radius_km, radius_km)
    # Random bearing
    angle = random.uniform(0, 2 * math.pi)

    new_lat_rad = math.asin(math.sin(base_lat_rad) * math.cos(distance_km / R) +
                           math.cos(base_lat_rad) * math.sin(distance_km / R) * math.cos(angle))
    new_lon_rad = base_lon_rad + math.atan2(math.sin(angle) * math.sin(distance_km / R) * math.cos(base_lat_rad),
                                           math.cos(distance_km / R) - math.sin(base_lat_rad) * math.sin(new_lat_rad))

    return round(math.degrees(new_lat_rad), 6), round(math.degrees(new_lon_rad), 6)

def reverse_geocode(lat, lon):
    """
    Reverse-geocodes coordinates to a human-readable address using Google Maps API.
    """
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lon}&key={GOOGLE_API_KEY}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "OK" and data.get("results"):
            return data["results"][0].get("formatted_address", "No address found")
        elif data.get("status") == "OVER_QUERY_LIMIT":
            return "GEOCODE_ERROR: API quota exceeded"
        elif data.get("status") == "REQUEST_DENIED":
            return f"GEOCODE_ERROR: {data.get('error_message', 'Request denied')}"
        else:
            return f"GEOCODE_ERROR: {data.get('status', 'Unknown error')}"
    except requests.exceptions.RequestException as e:
        return f"NETWORK_ERROR: {e}"

# --- Utility Functions ---

def _apply_geospatial_logic(df, filters_list):
    """
    Applies the geospatial logic to the DataFrame, updating locations for the first ride of the day and after breaks.
    """
    # Use English column names (as renamed in the endpoint)
    pickup_col = "pickup_location"
    geo_col = "geocoded_location"
    
    # Ensure these columns exist, otherwise add them
    if geo_col not in df.columns:
        df[geo_col] = None
    if pickup_col not in df.columns:
        df[pickup_col] = None

    # Create full datetime objects directly on the DataFrame
    df["ride_start_dt"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["ride_start"].dt.time.astype(str), errors="coerce"
    )
    df["ride_end_dt"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["ride_end"].dt.time.astype(str), errors="coerce"
    )

    df.dropna(subset=["ride_start_dt", "ride_end_dt"], inplace=True)
    if df.empty:
        return df

    indices_to_update = set()
    df_sorted = df.sort_values(by=["date", "ride_start_dt"])

    # Identify first ride of each day
    first_rides = df_sorted.groupby("date")["ride_start_dt"].idxmin()
    indices_to_update.update(first_rides)
    # Identify first ride after break
    for filter_data in filters_list:
        if filter_data.get("add_break"):
            try:
                filter_date = pd.to_datetime(filter_data["filter_date"]).date()
                break_end = pd.to_datetime(filter_data["break_end"])
                sub_df = df_sorted[
                    (df_sorted["date"] == filter_date)
                    & (df_sorted["ride_start_dt"] > break_end)
                ]
                if not sub_df.empty:
                    first_after_break = sub_df["ride_start_dt"].idxmin()
                    indices_to_update.add(first_after_break)
            except (KeyError, ValueError) as e:
                continue

    # Update coordinates and addresses for identified rides
    for idx in sorted(indices_to_update):
        # Generate new coordinates from base location
        new_lat, new_lon = _generate_random_location_in_radius(
            BASE_LOCATION_LATITUDE, BASE_LOCATION_LONGITUDE, RADIUS_KM, MIN_RADIUS_KM
        )

        # Respect Nominatim rate limit (1 request per second)
        time.sleep(1)

        # Reverse geocode the new coordinates
        address = reverse_geocode(new_lat, new_lon)

        # Update the DataFrame
        df.at[idx, geo_col] = f"{new_lat:.6f} {new_lon:.6f}"
        df.at[idx, pickup_col] = address if not address.startswith(("GEOCODE_ERROR", "NETWORK_ERROR")) else "GEOCODING_FAILED"

    # Clean up the temporary datetime columns before returning
    df.drop(columns=["ride_start_dt", "ride_end_dt"], errors='ignore', inplace=True)

    return df

# --- Dependency ---
async def get_current_user(token: str = Depends(oauth2_scheme)):
    """Validates the JWT token and returns the current user."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None or username not in USERS:
            raise credentials_exception
        return username
    except JWTError:
        raise credentials_exception

# --- API Endpoints ---
@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = USERS.get(form_data.username)
    if not user or not pwd_context.verify(form_data.password, user["password"]):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": form_data.username}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/check-auth")
async def check_auth(current_user: str = Depends(get_current_user)):
    return {"message": "Authenticated", "username": current_user}

@app.post("/filter-driver-batch")
async def filter_driver_batch(
    file: UploadFile = File(...),
    driver_name: str = Form(...),
    filters: str = Form(...),
    current_user: str = Depends(get_current_user),
):
    try:
        contents = await file.read()
        if len(contents) > 10 * 1024 * 1024:
            return JSONResponse(status_code=400, content={"error": "File size exceeds 10 MB limit."})

        df = pd.read_excel(BytesIO(contents))
        filters_list = json.loads(filters)

        # Mapping to English for processing
        column_mapping = {
            "datum der fahrt": "date",
            "fahrername": "driver_name",
            "uhrzeit des fahrtbeginns": "ride_start",
            "uhrzeit des fahrtendes": "ride_end",
            "abholort": "pickup_location",
            "standort des fahrzeugs bei auftragsuebermittlung": "geocoded_location",
            "abholzeit": "pickup_time",
        }
        
        # Create a reverse mapping for the final output
        reverse_column_mapping = {v: k for k, v in column_mapping.items()}
        
        df_proc = df.copy()
        
        # Normalize and rename columns for internal processing
        normalized_columns_map = {
            col: column_mapping.get(col.strip().lower(), col) for col in df_proc.columns
        }
        df_proc.rename(columns=normalized_columns_map, inplace=True)
        
        # Add geocoded_location if it doesn't exist
        if "geocoded_location" not in df_proc.columns:
            df_proc["geocoded_location"] = None

        required_columns = {"driver_name", "ride_start", "ride_end", "date", "pickup_location", "geocoded_location"}
        missing_columns = required_columns - set(df_proc.columns)
        if missing_columns:
            return JSONResponse(
                status_code=400,
                content={"error": f"Excel file missing required columns: {', '.join(missing_columns)}"}
            )
        
        # Data type conversion and cleaning
        for col in ["ride_start", "ride_end"]:
            df_proc[col] = pd.to_datetime(df_proc[col], errors="coerce", format=None) 
        df_proc["date"] = pd.to_datetime(df_proc["date"], errors="coerce").dt.date
        df_proc.dropna(subset=["ride_start", "ride_end", "driver_name", "date"], inplace=True)
        
        if df_proc.empty:
            return JSONResponse(status_code=404, content={"error": "No valid data after parsing datetimes."})

        # Apply driver name filter
        driver_name_clean = unicodedata.normalize("NFKD", driver_name.lower().strip())
        df_proc["driver_name_normalized"] = df_proc["driver_name"].astype(str).str.lower().str.strip().apply(lambda x: unicodedata.normalize("NFKD", x))
        final_df = df_proc[df_proc["driver_name_normalized"] == driver_name_clean].drop(columns=["driver_name_normalized"])

        if final_df.empty:
            return JSONResponse(
                status_code=404,
                content={"error": f"No data found for driver '{driver_name}'."}
            )
        
        # Apply break and off-day filters first
        for filter_data in filters_list:
            filter_date_str = filter_data.get("filter_date")
            if not filter_date_str:
                continue
            
            try:
                filter_date_dt = pd.to_datetime(filter_date_str, format="%Y-%m-%d").date()
                if filter_data.get("give_off"):
                    final_df = final_df[final_df["date"] != filter_date_dt]
                elif filter_data.get("add_break"):
                    break_start_str = filter_data.get("break_start")
                    break_end_str = filter_data.get("break_end")
                    if not break_start_str or not break_end_str:
                        continue
                    
                    break_start_dt = pd.to_datetime(break_start_str)
                    break_end_dt = pd.to_datetime(break_end_str)
                    
                    if (break_end_dt - break_start_dt).total_seconds() <= 0:
                        continue
                    
                    is_overlapping_break = (
                        (final_df["date"] == filter_date_dt) &
                        (final_df["ride_start"] <= break_end_dt) & 
                        (final_df["ride_end"] >= break_start_dt)
                    )
                    final_df = final_df[~is_overlapping_break]
            
            except Exception as e:
                continue
        
        if final_df.empty:
            return JSONResponse(
                status_code=404,
                content={"error": f"No data remains for driver '{driver_name}' after filtering."}
            )

        # Apply the new geospatial logic
        final_df = _apply_geospatial_logic(final_df.copy(), filters_list)

        # Recalculate hours worked and other final processing
        final_df["hours_worked"] = (final_df["ride_end"] - final_df["ride_start"]).dt.total_seconds() / 3600

        # Format dates back to string format for Excel output
        filtered = final_df.copy()
        for col in ["ride_start", "ride_end"]:
            filtered[col] = filtered[col].dt.strftime("%Y-%m-%d %H:%M:%S.000")
        if "date" in filtered.columns:
            filtered["date"] = pd.to_datetime(filtered["date"]).dt.strftime("%Y-%m-%d")

        # Rename columns back to original German headers
        final_columns = []
        for col in filtered.columns:
            final_columns.append(reverse_column_mapping.get(col, col))
        filtered.columns = final_columns

        # --- Create Excel Response ---
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            filtered.to_excel(writer, index=False, sheet_name="Sheet1")
            workbook = writer.book
            worksheet = writer.sheets["Sheet1"]

            for idx, col in enumerate(filtered.columns, 1):
                column_letter = get_column_letter(idx)
                col_lower = col.lower()
                if col_lower == reverse_column_mapping.get("date").lower():
                    worksheet.column_dimensions[column_letter].width = 15
                    for cell in worksheet[column_letter][1:]:
                        cell.number_format = "YYYY-MM-DD"
                elif col_lower in [reverse_column_mapping.get("ride_start").lower(), reverse_column_mapping.get("ride_end").lower()]:
                    worksheet.column_dimensions[column_letter].width = 25
                    for cell in worksheet[column_letter][1:]:
                        cell.number_format = "YYYY-MM-DD HH:MM:SS.000"
                elif col_lower == "hours_worked":
                    worksheet.column_dimensions[column_letter].width = 12
                    for cell in worksheet[column_letter][1:]:
                        cell.number_format = "0.00"
                elif col_lower == reverse_column_mapping.get("pickup_location").lower():
                    worksheet.column_dimensions[column_letter].width = 40
                elif col_lower == reverse_column_mapping.get("geocoded_location").lower():
                    worksheet.column_dimensions[column_letter].width = 30
                else:
                    worksheet.column_dimensions[column_letter].width = 20

        output.seek(0)
        return StreamingResponse(
            output,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": f"attachment; filename=filtered_combined_{driver_name_clean}.xlsx"}
        )

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Server error: {str(e)}"})