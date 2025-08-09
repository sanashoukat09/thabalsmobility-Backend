import os
import pandas as pd
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from io import BytesIO
from typing import Optional
import unicodedata
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

# Initialize FastAPI app
app = FastAPI()

# Configure CORS middleware to allow cross-origin requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root endpoint to check if backend is running
@app.get("/")
def read_root():
    return {"message": "Backend is running."}

# Endpoint to filter driver data from an uploaded Excel file
@app.post("/filter-driver")
async def filter_driver(
    file: UploadFile = File(...),
    driver_name: str = Form(...),
    add_break: bool = Form(False),
    give_off: bool = Form(False),
    break_start: Optional[str] = Form(None),
    break_end: Optional[str] = Form(None),
    off_date: Optional[str] = Form(None),
):
    try:
        # Validate uploaded file size (max 10 MB)
        if file.size > 10 * 1024 * 1024:
            return JSONResponse(status_code=400, content={"error": "File size exceeds 10 MB limit."})

        # Read Excel file into a pandas DataFrame
        contents = await file.read()
        df = pd.read_excel(BytesIO(contents))

        # Store original column names and create a copy for processing with normalized names
        original_columns = df.columns.tolist()
        df_proc = df.copy()
        normalized_columns = [col.strip().lower() for col in original_columns]
        df_proc.columns = normalized_columns

        print("? Uploaded Columns:", df_proc.columns.tolist())
        print(f"? Initial row count: {len(df_proc)}")

        # Define mapping of German column names to internal names for processing
        column_mapping = {
            "datum der fahrt": "date",
            "fahrername": "driver_name",
            "uhrzeit des fahrtbeginns": "ride_start",
            "uhrzeit des fahrtendes": "ride_end",
            "abholort": "pickup_location"
        }

        # Rename columns in processing DataFrame based on column_mapping
        for original, renamed in column_mapping.items():
            for col in df_proc.columns:
                if col.strip().lower() == original.strip().lower():
                    df_proc.rename(columns={col: renamed}, inplace=True)

        # Check for required columns in the processing DataFrame
        required_columns = {"driver_name", "ride_start", "ride_end", "date"}
        missing_columns = required_columns - set(df_proc.columns)
        if missing_columns:
            return JSONResponse(
                status_code=400,
                content={"error": f"Excel file missing required columns: {', '.join(missing_columns)}"}
            )

        # Get unique driver names for validation
        unique_drivers = df_proc["driver_name"].astype(str).str.lower().str.strip().unique().tolist()
        print(f"? Unique Fahrername values: {unique_drivers}")

        # Define supported datetime formats for parsing
        datetime_formats = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%H:%M:%S.%f",
            "%H:%M:%S",
            "%d.%m.%Y %H:%M:%S.%f",
            "%d/%m/%Y %H:%M:%S.%f",
            "%d.%m.%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d.%m.%Y %H:%M",
            "%d/%m/%Y %H:%M",
            "%d.%m.%Y",
            "%d/%m/%Y"
        ]

        # Parse ride_start and ride_end columns into datetime format
        for col in ["ride_start", "ride_end"]:
            for fmt in datetime_formats:
                df_proc[col] = pd.to_datetime(df_proc[col], format=fmt, errors="coerce")
                if not df_proc[col].isna().all():
                    break
            if df_proc[col].isna().all():
                print(f"?? All {col} values failed to parse with formats: {datetime_formats}")

        # Parse date column into date format
        df_proc["date"] = pd.to_datetime(df_proc["date"], format="%Y-%m-%d", errors="coerce").dt.date

        # Parse optional datetime column if present
        if "datetime" in df_proc.columns:
            for fmt in datetime_formats:
                df_proc["datetime"] = pd.to_datetime(df_proc["datetime"], format=fmt, errors="coerce")
                if not df_proc["datetime"].isna().all():
                    break
            if df_proc["datetime"].isna().all():
                print(f"?? All datetime (Datum/Uhrzeit) values failed to parse with formats: {datetime_formats}")

        # Remove rows with missing or invalid essential data
        invalid_rows = df_proc[["ride_start", "ride_end", "driver_name", "date"]].isna().any(axis=1)
        print(f"?? Rows with invalid datetimes: {invalid_rows.sum()}")
        if invalid_rows.any():
            print("?? Invalid rows sample:", df_proc[invalid_rows][["driver_name", "ride_start", "ride_end", "date"]].head().to_dict())
        df_proc.dropna(subset=["ride_start", "ride_end", "driver_name", "date"], inplace=True)
        print(f"? Rows after datetime parsing: {len(df_proc)}")

        # Check if DataFrame is empty after parsing
        if df_proc.empty:
            return JSONResponse(status_code=404, content={"error": "No valid data after parsing datetimes. Ensure Fahrername, Datum der Fahrt, ride_start, and ride_end have correct formats (e.g., YYYY-MM-DD for dates, HH:MM:SS.FFF for times)."})

        # Filter rows by normalized driver name
        driver_name_clean = unicodedata.normalize("NFKD", driver_name.lower().strip())
        df_proc["driver_name_normalized"] = df_proc["driver_name"].astype(str).str.lower().str.strip().apply(lambda x: unicodedata.normalize("NFKD", x))
        df_proc = df_proc[df_proc["driver_name_normalized"] == driver_name_clean]
        
        df_proc.drop(columns=["driver_name_normalized"], inplace=True)
        print(f"? Rows after driver filter ('{driver_name_clean}'): {len(df_proc)}")
        print(f"? Sample filtered rows: {df_proc[['driver_name', 'date', 'ride_start', 'ride_end']].head().to_dict()}")
        if df_proc.empty:
            return JSONResponse(
                status_code=404,
                content={"error": f"No data found for driver '{driver_name}'. Check spelling, case, whitespace, or special characters in Fahrername. Available drivers: {unique_drivers}."}
            )

        # Apply off-day filter if specified
        if give_off and off_date:
            try:
                off_date = pd.to_datetime(off_date, format="%Y-%m-%d").date()
                pre_off_rows = len(df_proc)
                df_proc = df_proc[df_proc["date"] != off_date]
                print(f"? Removed {pre_off_rows - len(df_proc)} rows for off day on {off_date}, remaining: {len(df_proc)}")
            except ValueError as e:
                return JSONResponse(status_code=400, content={"error": f"Invalid off date format. Use YYYY-MM-DD. Error: {e}"})

        # Apply break time filter if specified
        if add_break and break_start and break_end:
            try:
                break_formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]
                break_start_dt = None
                break_end_dt = None
                for fmt in break_formats:
                    try:
                        break_start_dt = pd.to_datetime(break_start, format=fmt, errors="raise")
                        break_end_dt = pd.to_datetime(break_end, format=fmt, errors="raise")
                        break
                    except ValueError:
                        continue
                if break_start_dt is None or break_end_dt is None:
                    raise ValueError("Invalid break time format. Use YYYY-MM-DD HH:MM:SS.FFF.")
                if (break_end_dt - break_start_dt).total_seconds() <= 0:
                    return JSONResponse(status_code=400, content={"error": "Break end time must be after start time."})
                pre_break_rows = len(df_proc)
                df_proc = df_proc[
                    ~(
                        (df_proc["date"] == break_start_dt.date()) &
                        (df_proc["ride_start"] <= break_end_dt) &
                        (df_proc["ride_end"] >= break_start_dt)
                    )
                ]
                print(f"? Removed {pre_break_rows - len(df_proc)} rows for break on {break_start_dt.date()} from {break_start_dt.time()} to {break_end_dt.time()}, remaining: {len(df_proc)}")
            except ValueError as e:
                return JSONResponse(status_code=400, content={"error": f"Invalid break time format. Use YYYY-MM-DD HH:MM:SS.FFF (24-hour). Error: {e}"})

        # Check if DataFrame is empty after filtering
        if df_proc.empty:
            return JSONResponse(
                status_code=404,
                content={"error": f"No data remains for driver '{driver_name}' after filtering. Check off date or break times against Datum der Fahrt and ride times, or verify '{driver_name}' has valid data."}
            )

        # Calculate hours worked per ride
        df_proc["hours_worked"] = (df_proc["ride_end"] - df_proc["ride_start"]).dt.total_seconds() / 3600

        # Update pickup location for the first ride of each day per driver
        if "pickup_location" in df_proc.columns:
            for (driver, date), group in df_proc.groupby(["driver_name", "date"]):
                if not group.empty:
                    earliest_ride_idx = group["ride_start"].idxmin()
                    df_proc.loc[earliest_ride_idx, "pickup_location"] = "Gladbacher Strasse 189, 41747 Viersen, Germany"
            print(f"? Updated Abholort for first rides: {df_proc[df_proc['pickup_location'] == 'Gladbacher Strasse 189, 41747 Viersen, Germany'].shape[0]} rows updated")
        else:
            print("?? Abholort column not found in input Excel. Skipping pickup location update.")

        # Prepare final DataFrame for export
        filtered = df_proc.copy()
        print(f"? Rows in filtered DataFrame: {len(filtered)}")
        print(f"? Output columns: {filtered.columns.tolist()}")

        # Format datetime columns to string with .000 milliseconds
        for col in ["ride_start", "ride_end"]:
            filtered[col] = filtered[col].dt.strftime("%Y-%m-%d %H:%M:%S.000")

        # Format date column to YYYY-MM-DD string
        if "date" in filtered.columns:
            filtered["date"] = pd.to_datetime(filtered["date"]).dt.strftime("%Y-%m-%d")


        # Restore original column names for output
        reverse_column_mapping = {v: k for k, v in column_mapping.items()}
        final_columns = []
        for col in filtered.columns:
            col_lower = col.lower()
            if col_lower in reverse_column_mapping:
                mapped_german_name = reverse_column_mapping[col_lower]
                original_col_name = None
                for orig_col in original_columns:
                    if orig_col.strip().lower() == mapped_german_name:
                        original_col_name = orig_col
                        break
                final_columns.append(original_col_name if original_col_name else mapped_german_name)
            else:
                final_columns.append(col)
        filtered.columns = final_columns

        # Export filtered DataFrame to Excel with custom formatting
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            filtered.to_excel(writer, index=False, sheet_name="Sheet1")
            workbook = writer.book
            worksheet = writer.sheets["Sheet1"]

            # Set column widths and formats based on column names
            for idx, col in enumerate(filtered.columns, 1):
                column_letter = get_column_letter(idx)
                col_lower = col.lower()
                if col_lower == "datum der fahrt":
                    worksheet.column_dimensions[column_letter].width = 15
                    for cell in worksheet[column_letter][1:]:
                        cell.number_format = "YYYY-MM-DD"
                elif col_lower in ["uhrzeit des fahrtbeginns", "uhrzeit des fahrtendes"]:
                    worksheet.column_dimensions[column_letter].width = 25
                    for cell in worksheet[column_letter][1:]:
                        cell.number_format = "YYYY-MM-DD HH:MM:SS.000"
                elif col_lower == "hours_worked":
                    worksheet.column_dimensions[column_letter].width = 12
                    for cell in worksheet[column_letter][1:]:
                        cell.number_format = "0.00"
                elif col_lower == "abholort":
                    worksheet.column_dimensions[column_letter].width = 40
                    for cell in worksheet[column_letter][1:]:
                        cell.number_format = "@"
                else:
                    worksheet.column_dimensions[column_letter].width = 20

        output.seek(0)

        # Return Excel file as streaming response
        return StreamingResponse(
            output,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": f"attachment; filename=filtered_{driver_name_clean}.xlsx"}
        )

    except Exception as e:
        print(f"? Error: {e}")
        return JSONResponse(status_code=500, content={"error": f"Server error: {str(e)}"})